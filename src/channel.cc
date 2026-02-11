#include <tll/channel/base.h>
#include <tll/channel/module.h>

#include <tll/scheme/util.h>
#include <tll/util/listiter.h>
#include <tll/util/memoryview.h>
#include <tll/util/decimal128.h>

#include <chrono>

#include <sql.h>
#include <sqlext.h>
#include <time.h>

#include "heartbeat.h"
#include "odbc-scheme.h"

using Channel = tll::Channel;
namespace dcaps { using namespace tll::dcaps; }

enum class Template { None, Insert, Function, Procedure };
template <>
struct tll::conv::parse<Template>
{
        static result_t<Template> to_any(std::string_view s)
        {
                return tll::conv::select(s, std::map<std::string_view, Template> {
			{"none", Template::None},
			{"insert", Template::Insert},
			{"function", Template::Function},
			{"procedure", Template::Procedure}
		});
        }
};

template <SQLSMALLINT Type>
struct SQLHandle
{
	class HandleType
	{
		HandleType() = delete;
	 public:
		static void operator delete (void *ptr) { SQLFreeHandle(Type, ptr); }
	};

	std::shared_ptr<HandleType> ptr;

	void reset(void * v = nullptr) { ptr.reset(static_cast<HandleType *>(v)); }

	operator bool () const { return ptr.get() != nullptr; }

	operator void * () { return ptr.get(); }
	operator const void * () const { return ptr.get(); }
};

using query_ptr_t = SQLHandle<SQL_HANDLE_STMT>;

struct Prepared
{
	Prepared(query_ptr_t && ptr) : sql(std::move(ptr)) {}
	query_ptr_t sql;
	const tll::scheme::Message * message = nullptr;
	const tll::scheme::Message * output_message = nullptr;
	Prepared * output = nullptr; // Non-null for function calls

	struct Convert {
		enum Type { None, String, Numeric, Timestamp } type = None;
		const tll::scheme::Field * field;
		SQLLEN param;
		union {
			int64_t integer;
			char * string;
			SQL_NUMERIC_STRUCT numeric;
			SQL_TIMESTAMP_STRUCT timestamp;
		};
	};
	std::vector<Convert> convert;
	bool with_seq;
};

namespace {
template <typename Iter>
std::string join(std::string_view sep, const Iter &begin, const Iter &end)
{
	std::string r;
	bool comma = false;
	for (Iter it = begin; it != end; ++it) {
		if (comma)
			r += sep;
		comma = true;
		r += tll::conv::to_string(*it);
	}
	return r;
}

template <typename Iter>
std::string join(const Iter &begin, const Iter &end)
{
	return join(", ", begin, end);
}

tll::result_t<std::string> sql_type(const tll::scheme::Field *field)
{
	using tll::scheme::Field;
	switch (field->sub_type) {
	case Field::TimePoint:
		return "TIMESTAMP";
	default:
		break;
	}
	switch (field->type) {
	case Field::Int8:
	case Field::UInt8:
	case Field::Int16:
	case Field::UInt16:
	case Field::Int32:
		return "INTEGER";
	case Field::UInt32:
	case Field::Int64:
		return "BIGINT";
	case Field::UInt64:
		return tll::error("UInt64 not supported");

	case Field::Double:
		return "REAL";

	case Field::Decimal128:
		return "NUMERIC";

	case Field::Bytes:
		if (field->sub_type == Field::ByteString)
			return fmt::format("VARCHAR({})", field->size);
		return tll::error("Blobs not supported");

	case Field::Message:
		return tll::error("Nested messages not supported");
	case Field::Array:
		return tll::error("Nested arrays not supported");
	case Field::Pointer:
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			return "VARCHAR";
		}
		return tll::error("Nested arrays not supported");
	case Field::Union:
		return tll::error("Union not supported");
	}
	return tll::error("Invalid field type");
}

template <typename T, typename Res>
std::pair<time_t, unsigned> split_time(const T * data)
{
	using namespace std::chrono;
	auto ts = (const duration<T, Res> *) data;
	auto seconds = duration_cast<duration<time_t, std::ratio<1>>>(*ts);
	auto sub = duration_cast<duration<unsigned, std::nano>>(*ts - seconds);
	return { seconds.count(), sub.count() };
}

template <typename T, typename Res>
T compose_time(time_t seconds, unsigned ns)
{
	using namespace std::chrono;
	auto ts = duration_cast<duration<T, Res>>(std::chrono::seconds { seconds });
	ts += duration_cast<duration<T, Res>>(duration<unsigned, std::nano>(ns));
	return ts.count();
}

template <typename T, typename Buf>
int write_time(tll::Logger &_log, const Prepared::Convert & convert, Buf data)
{
	using tll::scheme::Field;
	struct tm ctm;
	const auto & sqlts = convert.timestamp;
	ctm.tm_year = sqlts.year - 1900;
	ctm.tm_mon = sqlts.month - 1;
	ctm.tm_mday = sqlts.day;
	ctm.tm_hour = sqlts.hour;
	ctm.tm_min = sqlts.minute;
	ctm.tm_sec = sqlts.second;
	auto seconds = timegm(&ctm);
	if (seconds == -1)
		return _log.fail(EINVAL, "Failed to convert timestamp {}-{}-{}: {}", sqlts.year, sqlts.month, sqlts.day, strerror(errno));

	T value;
	switch (convert.field->time_resolution) {
	case TLL_SCHEME_TIME_NS: value = compose_time<T, std::nano>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_US: value = compose_time<T, std::micro>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_MS: value = compose_time<T, std::milli>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_SECOND: value = compose_time<T, std::ratio<1>>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_MINUTE: value = compose_time<T, std::ratio<60>>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_HOUR: value = compose_time<T, std::ratio<3600>>(seconds, sqlts.fraction); break;
	case TLL_SCHEME_TIME_DAY: value = compose_time<T, std::ratio<86400>>(seconds, sqlts.fraction); break;
	default:
		return _log.fail(EINVAL, "Unknown time resolution: {}", (int) convert.field->time_resolution);
	}
	*data.template dataT<T>() = value;
	return 0;
}

template <typename T>
int sql_bind_numeric(SQLHSTMT sql, int idx, int ctype, int sqltype, const T * data, Prepared::Convert &convert)
{
	using tll::scheme::Field;
	if (convert.field->sub_type == Field::TimePoint) {
		std::pair<time_t, unsigned> parts = {};
		switch (convert.field->time_resolution) {
		case TLL_SCHEME_TIME_NS: parts = split_time<T, std::nano>(data); break;
		case TLL_SCHEME_TIME_US: parts = split_time<T, std::micro>(data); break;
		case TLL_SCHEME_TIME_MS: parts = split_time<T, std::milli>(data); break;
		case TLL_SCHEME_TIME_SECOND: parts = split_time<T, std::ratio<1>>(data); break;
		case TLL_SCHEME_TIME_MINUTE: parts = split_time<T, std::ratio<60>>(data); break;
		case TLL_SCHEME_TIME_HOUR: parts = split_time<T, std::ratio<3600>>(data); break;
		case TLL_SCHEME_TIME_DAY: parts = split_time<T, std::ratio<86400>>(data); break;
		}
		struct tm result;
		if (!gmtime_r(&parts.first, &result))
			return EOVERFLOW;
		convert.timestamp.year = 1900 + result.tm_year;
		convert.timestamp.month = 1 + result.tm_mon;
		convert.timestamp.day = result.tm_mday;
		convert.timestamp.hour = result.tm_hour;
		convert.timestamp.minute = result.tm_min;
		convert.timestamp.second = result.tm_sec;
		convert.timestamp.fraction = parts.second;
		convert.param = sizeof(convert.timestamp);
		return SQLBindParam(sql, idx, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 0, 0, (SQLPOINTER) &convert.timestamp, &convert.param);
	}
	return SQLBindParam(sql, idx, ctype, sqltype, 0, 0, (SQLPOINTER) data, &convert.param);
}

template <typename Buf>
int sql_bind(SQLHSTMT sql, Prepared::Convert &convert, int idx, const Buf &data)
{
	using tll::scheme::Field;
	switch (convert.field->type) {
	case Field::Int8:
		return sql_bind_numeric(sql, idx, SQL_C_STINYINT, SQL_SMALLINT, data.template dataT<int8_t>(), convert);
	case Field::Int16:
		return sql_bind_numeric(sql, idx, SQL_C_SSHORT, SQL_INTEGER, data.template dataT<int16_t>(), convert);
	case Field::Int32:
		return sql_bind_numeric(sql, idx, SQL_C_SLONG, SQL_INTEGER, data.template dataT<int32_t>(), convert);
	case Field::Int64:
		return sql_bind_numeric(sql, idx, SQL_C_SBIGINT, SQL_BIGINT, data.template dataT<int64_t>(), convert);
	case Field::UInt8:
		return sql_bind_numeric(sql, idx, SQL_C_UTINYINT, SQL_SMALLINT, data.template dataT<uint8_t>(), convert);
	case Field::UInt16:
		return sql_bind_numeric(sql, idx, SQL_C_USHORT, SQL_INTEGER, data.template dataT<uint16_t>(), convert);
	case Field::UInt32:
		return sql_bind_numeric(sql, idx, SQL_C_ULONG, SQL_BIGINT, data.template dataT<uint32_t>(), convert);

	case Field::Double:
		return sql_bind_numeric(sql, idx, SQL_C_DOUBLE, SQL_DOUBLE, data.template dataT<double>(), convert);

	case Field::UInt64:
		return SQL_ERROR;

	case Field::Decimal128: {
		if (convert.param == SQL_NULL_DATA)
			return SQLBindParam(sql, idx, SQL_C_NUMERIC, SQL_NUMERIC, 0, 0, &convert.numeric, &convert.param);
		auto & n = convert.numeric;
		tll::util::Decimal128::Unpacked u128;
		data.template dataT<tll::util::Decimal128>()->unpack(u128);

		memcpy(n.val, &u128.mantissa, sizeof(u128.mantissa));
		n.precision = 34;
		n.scale = -u128.exponent;
		n.sign = u128.sign ? 0 : 1;
		convert.param = sizeof(n);
		if (auto r = SQLBindParam(sql, idx, SQL_C_NUMERIC, SQL_NUMERIC, n.precision, n.scale, &n, &convert.param))
			return r;

		SQLHDESC desc = nullptr;
		const intptr_t scale = n.scale;
		const intptr_t precision = n.precision;
		SQLGetStmtAttr(sql, SQL_ATTR_APP_PARAM_DESC, &desc, 0, NULL);
		SQLSetDescField(desc, idx, SQL_DESC_TYPE, (SQLPOINTER) SQL_C_NUMERIC, 0);
		SQLSetDescField(desc, idx, SQL_DESC_PRECISION, (SQLPOINTER) precision, 0);
		SQLSetDescField(desc, idx, SQL_DESC_SCALE, (SQLPOINTER) scale, 0);
		SQLSetDescField(desc, idx, SQL_DESC_DATA_PTR, (SQLPOINTER) &n, 0);
		return 0;
	}

	case Field::Bytes:
		if (convert.field->sub_type == Field::ByteString) {
			if (convert.param == SQL_NULL_DATA)
				return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) convert.string, &convert.param);
			auto str = data.template dataT<char>();
			convert.param = strnlen(str, convert.field->size);
			return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) str, &convert.param);
		}
		return SQL_ERROR;

	case Field::Message:
		return SQL_ERROR;
	case Field::Array:
		return SQL_ERROR;
	case Field::Pointer:
		if (convert.field->type_ptr->type == Field::Int8 && convert.field->sub_type == Field::ByteString) {
			if (convert.param == SQL_NULL_DATA)
				return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) convert.string, &convert.param);
			auto ptr = tll::scheme::read_pointer(convert.field, data);
			if (!ptr)
				return SQL_ERROR;
			if (ptr->size == 0)
				return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) "", &convert.param);
			convert.param = ptr->size - 1;
			return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) data.view(ptr->offset).template dataT<char>(), &convert.param);
		}
		return SQL_ERROR;
	case Field::Union:
		return SQL_ERROR;
	}
	return SQL_ERROR;
}

template <typename Buf>
int sql_column(SQLHSTMT sql, Prepared::Convert &convert, int idx, const tll::scheme::Field *field, Buf data)
{
	switch (convert.type) {
	case Prepared::Convert::None:
		break;
	case Prepared::Convert::Numeric:
		return SQLBindCol(sql, idx, SQL_C_NUMERIC, (SQLPOINTER) &convert.numeric, sizeof(SQL_NUMERIC_STRUCT), &convert.param);
	case Prepared::Convert::Timestamp:
		return SQLBindCol(sql, idx, SQL_C_TYPE_TIMESTAMP, (SQLPOINTER) &convert.timestamp, sizeof(convert.timestamp), &convert.param);
	case Prepared::Convert::String:
		return SQLBindCol(sql, idx, SQL_C_CHAR, convert.string, 1024, &convert.param);
	}

	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
		return SQLBindCol(sql, idx, SQL_C_STINYINT, data.data(), sizeof(int8_t), &convert.param);
	case Field::Int16:
		return SQLBindCol(sql, idx, SQL_C_SSHORT, data.data(), sizeof(int16_t), &convert.param);
	case Field::Int32:
		return SQLBindCol(sql, idx, SQL_C_SLONG, data.data(), sizeof(int32_t), &convert.param);
	case Field::Int64:
		return SQLBindCol(sql, idx, SQL_C_SBIGINT, data.data(), sizeof(int64_t), &convert.param);
	case Field::UInt8:
		return SQLBindCol(sql, idx, SQL_C_UTINYINT, data.data(), sizeof(uint8_t), &convert.param);
	case Field::UInt16:
		return SQLBindCol(sql, idx, SQL_C_USHORT, data.data(), sizeof(uint16_t), &convert.param);
	case Field::UInt32:
		return SQLBindCol(sql, idx, SQL_C_ULONG, data.data(), sizeof(uint32_t), &convert.param);
	case Field::UInt64:
		return SQL_ERROR;

	case Field::Double:
		return SQLBindCol(sql, idx, SQL_C_DOUBLE, data.data(), sizeof(double), &convert.param);

	case Field::Decimal128:
		return SQL_ERROR; // Already handled

	case Field::Bytes:
		if (field->sub_type == Field::ByteString) {
			return SQLBindCol(sql, idx, SQL_C_CHAR, data.data(), field->size, &convert.param);
		}
		return SQL_ERROR;

	case Field::Message:
		return SQL_ERROR;
	case Field::Array:
		return SQL_ERROR;
	case Field::Pointer:
		return SQL_ERROR; // Already handled or unsupported
	case Field::Union:
		return SQL_ERROR;
	}
	return SQL_ERROR;
}

}

class ODBC : public tll::channel::Base<ODBC>
{
	using Base = tll::channel::Base<ODBC>;

	SQLHandle<SQL_HANDLE_ENV> _env;
	SQLHandle<SQL_HANDLE_DBC> _db;

	query_ptr_t _select_sql;
	Prepared * _select = nullptr;

	std::string _settings;
	std::vector<char> _buf;
	std::list<std::vector<char>> _string_buffers;
	std::vector<char> _errorbuf;
	std::string_view _sqlstate;

	std::map<int, Prepared> _messages;

	SQLLEN _seq_param;
	tll_msg_t _msg = {};

	Template _default_template = Template::Insert;

	enum class Index { No, Yes, Unique } _seq_index = Index::Unique;
	enum class Create { No, Checked, Always } _create_mode = Create::Checked;
	enum class Quotes { SQLite, PSQL, Sybase, None } _quotes = Quotes::PSQL;
	enum class Function { Fields, Empty } _function_mode = Function::Fields;

 public:
	static constexpr auto process_policy() { return ProcessPolicy::Custom; }

	static constexpr std::string_view channel_protocol() { return "odbc"; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();
	//void _destroy();

	int _post(const tll_msg_t *msg, int flags);
	int _post_control(const tll_msg_t *msg, int flags);
	int _process(long timeout, int flags);

 private:
	int _create_table(std::string_view table, const tll::scheme::Message *);
	int _create_query(const tll::scheme::Message *);
	int _create_index(const std::string_view &name, std::string_view key, bool unique);

	int _execute(query_ptr_t &query, std::string_view message);

	std::string _quoted(std::string_view name) // Can only be used for table/field names, no escaping performed
	{
		switch (_quotes) {
		case Quotes::SQLite: return fmt::format("`{}`", name);
		case Quotes::PSQL: return fmt::format("\"{}\"", name);
		case Quotes::Sybase: return fmt::format("[{}]", name);
		case Quotes::None: return std::string(name);
		}
		return std::string(name);
	}

	std::string _quoted_table(std::string_view name)
	{
		auto dot = name.find('.');
		if (dot == name.npos)
			return _quoted(name);
		return _quoted(name.substr(0, dot)) + "." + _quoted(name.substr(dot + 1));
	}

	std::string_view _if_not_exists()
	{
		if (_create_mode == Create::Checked)
			return "IF NOT EXISTS ";
		return "";
	}

	query_ptr_t _prepare(const std::string_view query)
	{
		_log.debug("Prepare SQL statement:\n\t{}", query);
		SQLHSTMT ptr;
		if (auto r = SQLAllocHandle(SQL_HANDLE_STMT, _db, &ptr); r != SQL_SUCCESS)
			return _log.fail(query_ptr_t {}, "Failed to allocate statement: {}\n\t{}", odbcerror(_db), query);
		query_ptr_t sql;
		sql.reset(ptr);
		if (auto r = SQLPrepare(sql, (SQLCHAR *) query.data(), query.size()); r != SQL_SUCCESS)
			return _log.fail(query_ptr_t {}, "Failed to prepare statement: {}\n\t{}", odbcerror(sql), query);
		return sql;
	}

	template <SQLSMALLINT Type>
	std::string_view odbcerror(SQLHandle<Type> &handle) { return _odbcerror(Type, handle); }

	std::string_view _odbcerror(const SQLSMALLINT type, void *handle)
	{
		auto view = tll::make_view(_errorbuf);
		_sqlstate = "";

		for (auto i = 1; ; i++) {
			if (view.size() < 9)
				view.resize(9); // One byte for internal strnlen inside SQLGetDiagRec
			view.dataT<char>()[0] = 0;
			view.dataT<char>()[8] = 0;
			SQLSMALLINT len = 0;
			SQLINTEGER native;
			auto sqlstate = view.view(1);
			auto buf = view.view(1 + 5 + 2); //'\nHYXXX: ' string
			auto r = SQLGetDiagRec(type, handle, i, sqlstate.dataT<SQLCHAR>(), &native, buf.dataT<SQLCHAR>(), buf.size(), &len);
			if (!SQL_SUCCEEDED(r))
				break;
			if (buf.size() < (size_t) len) {
				buf.resize(len + 1);
				len = 0;
				r = SQLGetDiagRec(type, handle, i, sqlstate.dataT<SQLCHAR>(), &native, buf.dataT<SQLCHAR>(), buf.size(), &len);
			}
			if (!SQL_SUCCEEDED(r))
				break;
			if (len == 0) // Postgres return empty error message instead of non-success error code
				break;
			view.dataT<char>()[0] = '\n';
			view.dataT<char>()[6] = ':';
			view.dataT<char>()[7] = ' ';
			view = buf.view(len);
		}

		auto size = view.dataT<char>() - _errorbuf.data();
		if (size == 0)
			return "";
		_sqlstate = std::string_view(_errorbuf.data() + 1, 5);
		return std::string_view(_errorbuf.data() + 1, size - 1); // Skip first delimiter
	}
};

int ODBC::_init(const Channel::Url &url, Channel * master)
{
	if (!_scheme_url)
		return _log.fail(EINVAL, "ODBC channel needs scheme");

	std::map<std::string, std::string, std::less<>> settings;
	auto reader = channel_props_reader(url);
	for (auto &k : std::array<std::string_view, 3> { "dsn", "driver", "database"}) {
		auto v = reader.getT<std::string>(k, "");
		if (v.size())
			settings.emplace(k, v);
	}

	_default_template = reader.getT("default-template", _default_template);
	_create_mode = reader.getT("create-mode", Create::No, {{"no", Create::No}, {"checked", Create::Checked}, {"always", Create::Always}});
	_quotes = reader.getT("quote-mode", Quotes::PSQL, {{"sqlite", Quotes::SQLite}, {"psql", Quotes::PSQL}, {"sybase", Quotes::Sybase}, {"none", Quotes::None}});
	_function_mode = reader.getT("function-mode", Function::Fields, {{"fields", Function::Fields}, {"empty", Function::Empty}});
	if (!reader)
		return _log.fail(EINVAL, "Invalid url: {}", reader.error());

	if (auto sub = url.sub("settings"); sub) {
		for (auto &[k, c] : sub->browse("*")) {
			auto v = c.get();
			if (!v || !v->size()) continue;
			settings.emplace(k, *v);
		}
	}
	if (auto sub = url.sub("odbc.settings"); sub) {
		for (auto &[k, c] : sub->browse("*")) {
			auto v = c.get();
			if (!v || !v->size()) continue;
			settings.emplace(k, *v);
		}
	}

	if (settings.empty())
		return _log.fail(EINVAL, "No settings for ODBC driver");

	for (auto &[k, v]: settings) {
		if (_settings.size())
			_settings += ";";
		_settings += fmt::format("{}={}", k, v);
	}
	_log.info("Connection string: {}", _settings);

	_scheme_control.reset(context().scheme_load(odbc_scheme::scheme_string));
	if (!_scheme_control.get())
		return _log.fail(EINVAL, "Failed to load odbc control scheme");

	_errorbuf.resize(512);

	return Base::_init(url, master);
}

int ODBC::_open(const tll::ConstConfig &s)
{
	if (auto r = Base::_open(s); r)
		return _log.fail(r, "Failed to open ODBC database");

	SQLHENV henv = nullptr;
	if (auto r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to allocate ODBC Environment: {}", r);
	_env.reset(henv);

        if (auto r = SQLSetEnvAttr(_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to request ODBCv3: {}", odbcerror(_env));

	SQLHDBC hdbc = nullptr;
	if (auto r = SQLAllocHandle(SQL_HANDLE_DBC, _env, &hdbc); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to allocate ODBC Connection: {}", odbcerror(_env));
	_db.reset(hdbc);

	char buf[SQL_MAX_OPTION_STRING_LENGTH];
	SQLSMALLINT buflen = sizeof(buf);
	if (auto r = SQLDriverConnect (_db, nullptr, (SQLCHAR *) _settings.data(), _settings.size(),
                               (SQLCHAR *) buf, sizeof(buf), &buflen, SQL_DRIVER_NOPROMPT); !SQL_SUCCEEDED(r)) {
		return _log.fail(EINVAL, "Failed to connect: {}\n\tConnection string: {}", odbcerror(_db), _settings);
	}
	_log.info("Connection string: {}", buf); //std::string_view(buf, buflen));

	for (auto & m : tll::util::list_wrap(_scheme->messages)) {
		if (m.msgid == 0) {
			_log.debug("Message {} has no msgid, skip table check", m.name);
			continue;
		}

		if (_create_query(&m))
			return _log.fail(EINVAL, "Failed to prepare SQL statement for '{}'", m.name);
	}

	for (auto & [_, m] : _messages) {
		using tll::scheme::Field;
		if (m.output_message) {
			auto it = _messages.find(m.output_message->msgid);
			if (it == _messages.end())
				return _log.fail(EINVAL, "Output message {} was not prepared", m.output_message->name);
			m.output = &it->second;
		}
		auto ibuf = _string_buffers.begin();
		auto i = 0;
		for (auto & f : tll::util::list_wrap(m.message->fields)) {
			if (&f == m.message->pmap)
				continue;
			auto & conv = m.convert[i++];
			conv.field = &f;
			if (f.type == Field::Pointer && f.type_ptr->type == Field::Int8 && f.sub_type == Field::ByteString) {
				conv.type = Prepared::Convert::String;
				if (ibuf == _string_buffers.end()) {
					_string_buffers.push_back({});
					ibuf = --_string_buffers.end();
				}
				ibuf->resize(1024);
				conv.string = ibuf->data();
				ibuf++;
			} else if (f.type == Field::Decimal128) {
				conv.type = Prepared::Convert::Numeric;
			} else if (f.sub_type == Field::TimePoint) {
				conv.type = Prepared::Convert::Timestamp;
			}
		}
	}

	return 0;
}

int ODBC::_close()
{
	_messages.clear();
	_select_sql.reset();
	if (_db.ptr)
		SQLDisconnect(_db);
	_db.reset();
	_env.reset();
	return Base::_close();
}

int ODBC::_create_table(std::string_view table, const tll::scheme::Message * msg)
{
	query_ptr_t sql;

	_log.info("Create table '{}'", table);
	std::list<std::string> fields;

	auto with_seq = tll::getter::getT(msg->options, "sql.with-seq", true);
	if (!with_seq)
		return _log.fail(EINVAL, "Invalid sql.with-seq option: {}", with_seq.error());
	if (*with_seq)
		fields.push_back(fmt::format("{} INTEGER", _quoted("_tll_seq")));

	for (auto & f : tll::util::list_wrap(msg->fields)) {
		if (&f == msg->pmap)
			continue;
		auto options = f.options;
		if (f.type == f.Pointer)
			options = f.type_ptr->options;

		auto t = sql_type(&f);
		if (!t)
			return _log.fail(EINVAL, "Message {} field {}: {}", msg->name, f.name, t.error());

		auto otype = tll::getter::get(options, "sql.column-type").value_or(*t);
		std::string_view notnull = " NOT NULL";
		if (msg->pmap && f.index >= 0)
			notnull = "";
		fields.push_back(fmt::format("{} {}{}", _quoted(f.name), otype, notnull));

		auto pkey = tll::getter::getT(options, "sql.primary-key", false);

		if (!pkey)
			_log.warning("Invalid primary-key option: {}", pkey.error());
		else if (*pkey) {
			_log.debug("Field {} is primary key", f.name);
			fields.back() += " PRIMARY KEY";
		}
	}

	sql = _prepare(fmt::format("CREATE TABLE {}{} ({})", _if_not_exists(), _quoted_table(table), join(fields.begin(), fields.end())));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare CREATE statement");

	if (auto r = SQLExecute(sql); r != SQL_SUCCESS && r != SQL_NO_DATA)
		return _log.fail(EINVAL, "Failed to create table '{}': {}", table, odbcerror(sql));

	{
		auto index = tll::getter::getT(msg->options, "sql.index", _seq_index, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
		if (!index) {
			_log.warning("Invalid sql.index option for {}: {}", msg->name, index.error());
		} else if (!*with_seq) { // No seq field
		} else if (*index != Index::No) {
			if (_create_index(table, "_tll_seq", *index == Index::Unique))
				return _log.fail(EINVAL, "Failed to create seq index for table {}", table);
		}
	}

	for (auto & f : tll::util::list_wrap(msg->fields)) {
		auto index = tll::getter::getT(f.options, "sql.index", Index::No, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
		if (!index) {
			_log.warning("Invalid sql.index option for {}.{}: {}", msg->name, f.name, index.error());
		} else if (*index != Index::No) {
			if (_create_index(table, f.name, *index == Index::Unique))
				return _log.fail(EINVAL, "Failed to create index {} for table {}", f.name, table);
		}
	}

	return 0;
}

int ODBC::_create_query(const tll::scheme::Message *msg)
{
	auto reader = tll::make_props_reader(msg->options);

	auto table = reader.getT<std::string>("sql.table", msg->name);

	std::list<std::string> names;
	auto with_seq = reader.getT("sql.with-seq", true);

	if (with_seq)
		names.push_back(_quoted("_tll_seq"));

	for (auto & f : tll::util::list_wrap(msg->fields)) {
		if (&f == msg->pmap)
			continue;
		names.push_back(_quoted(f.name));
	}

	auto tmpl = reader.getT("sql.template", _default_template);
	auto query = reader.getT("sql.query", std::string());
	auto output = reader.getT("sql.output", std::string());

	if (query.size())
		tmpl = Template::None;

	auto create = reader.getT("sql.create", tmpl == Template::Insert);

	if (!reader)
		return _log.fail(EINVAL, "Failed to read SQL options from message '{}': {}", msg->name, reader.error());

	const tll::scheme::Message * outmsg = nullptr;
	if (output.size()) {
		outmsg = _scheme->lookup(output);
		if (!outmsg)
			return _log.fail(EINVAL, "Output message '{}' for query '{}' not found", output, msg->name);
	}

	switch (tmpl) {
	case Template::None:
		break;
	case Template::Insert:
		query = fmt::format("INSERT INTO {}({}) VALUES ", _quoted_table(table), join(names.begin(), names.end()));
		for (auto & i : names)
			i = "?";
		query += fmt::format("({})", join(names.begin(), names.end()));
		break;
	case Template::Function: {
		if (!outmsg)
			return _log.fail(EINVAL, "Function template '{}' without output message", msg->name);
		std::list<std::string> outnames;
		for (auto & f : tll::util::list_wrap(outmsg->fields)) {
			if (&f == outmsg->pmap)
				continue;
			outnames.push_back(_quoted(f.name));
		}
		for (auto & i : names)
			i = "?";
		if (_function_mode == Function::Fields)
			query = fmt::format("SELECT {} FROM", join(outnames.begin(), outnames.end()));
		else
			query = "SELECT";
		query += fmt::format(" {}({})", _quoted_table(table), join(names.begin(), names.end()));
		break;
	}
	case Template::Procedure:
		for (auto & i : names)
			i = "?";
		query = fmt::format("CALL {}({})", _quoted_table(table), join(names.begin(), names.end()));
		break;
	}

	query_ptr_t sql;
	if (query.size()) {
		sql = _prepare(query);
		if (!sql)
			return _log.fail(EINVAL, "Failed to prepare insert statement for table {}: {}", table, query);
	}

	if (create && _create_mode != Create::No) {
		if (_create_table(table, msg))
			return _log.fail(EINVAL, "Failed to create table '{}' for '{}'", table, msg->name);
	}

	auto it = _messages.emplace(msg->msgid, std::move(sql)).first;
	it->second.message = msg;
	it->second.convert.resize(with_seq ? names.size() - 1 : names.size());
	it->second.output_message = outmsg;
	it->second.with_seq = with_seq;

	return 0;
}

int ODBC::_create_index(const std::string_view &name, std::string_view key, bool unique)
{
	_log.debug("Create index for {}: key {}", name, key);
	query_ptr_t sql;

	std::string_view ustr = unique ? "UNIQUE " : "";
	auto str = fmt::format("CREATE {} INDEX {}{} on {}({})", ustr, _if_not_exists(), _quoted(fmt::format("_tll_{}_{}", name, key)), _quoted(name), _quoted(key));
	sql = _prepare(str);
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare index statement: {}", str);

	if (auto r = SQLExecute(sql); r != SQL_SUCCESS && r != SQL_NO_DATA)
		return _log.fail(EINVAL, "Failed to create index for '{}': {}", name, key);
	return 0;
}

int ODBC::_post(const tll_msg_t *msg, int flags)
{
	if (msg->type != TLL_MESSAGE_DATA) {
		if (msg->type == TLL_MESSAGE_CONTROL)
			return _post_control(msg, flags);
		return 0;
	}

	if (_select_sql)
		return _log.fail(EINVAL, "Previous query is not finished, can not write data");

	if (msg->msgid == 0)
		return _log.fail(EINVAL, "Unable to insert message without msgid");
	auto it = _messages.find(msg->msgid);
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found", msg->msgid);
	auto & insert = it->second;

	if (!insert.sql) {
		_log.trace("Skip message {} without SQL statement", insert.message->name);
		return 0;
	}

	SQLFreeStmt(insert.sql, SQL_RESET_PARAMS);

	auto view = tll::make_view(*msg);

	int idx = 1;
	if (insert.with_seq) {
		if (auto r = SQLBindParam(insert.sql, idx++, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) &msg->seq, &_seq_param); !SQL_SUCCEEDED(r))
			return _log.fail(EINVAL, "Failed to bind seq: {}", odbcerror(insert.sql));
	}
	auto pmap = insert.message->pmap;
	for (auto & c : insert.convert) {
		if (pmap && c.field->index >= 0) {
			if (tll_scheme_pmap_get(view.view(pmap->offset).data(), c.field->index))
				c.param = 0;
			else
				c.param = SQL_NULL_DATA;
		}
		if (sql_bind(insert.sql, c, idx++, view.view(c.field->offset)))
			return _log.fail(EINVAL, "Failed to bind field {}: {}", c.field->name, odbcerror(insert.sql));
	}

	if (auto r = _execute(insert.sql, "insert"); r) {
		if (r == ENOENT) {
			if (!insert.output)
				return 0;
			tll_msg_t msg = {
				.type = TLL_MESSAGE_CONTROL,
				.msgid = odbc_scheme::EndOfData::meta_id(),
			};
			_callback(&msg);
			return 0;
		}
		return r;
	}

	if (!insert.output) {
		SQLCloseCursor(insert.sql);
		return 0;
	}

	{
		_select_sql = insert.sql;
		_select = insert.output;

		auto view = tll::make_view(_buf);
		_buf.resize(0);
		_buf.resize(_select->message->size);
		_buf.reserve(65536);

		idx = 1;
		if (_select->with_seq) {
			if (auto r = SQLBindCol(_select_sql, idx++, SQL_C_SBIGINT, &_msg.seq, sizeof(_msg.seq), &_seq_param); !SQL_SUCCEEDED(r))
				return _log.fail(EINVAL, "Failed to bind seq column: {}", odbcerror(_select_sql));
		}
		for (auto & c : _select->convert) {
			_log.debug("Bind field {} at {}", c.field->name, c.field->offset);
			if (sql_column(_select_sql, c, idx++, c.field, view.view(c.field->offset)))
				return _log.fail(EINVAL, "Failed to bind field {} column: {}", c.field->name, odbcerror(_select_sql));
		}

		_update_dcaps(dcaps::Process | dcaps::Pending);
	}
	return 0;
}

int ODBC::_execute(query_ptr_t &query, std::string_view message)
{
	if (auto r = SQLExecute(query); !SQL_SUCCEEDED(r)) {
		auto error = odbcerror(query);
		if (_sqlstate == "08S01") // Fatal connection error
			return state_fail(EINVAL, "Failed to {} data: {}", message, error);
		if (r == SQL_NO_DATA) {
			_log.debug("Query returned no data (SQL_NO_DATA)");
			return ENOENT;
		}
		if (r == SQL_NEED_DATA)
			return _log.fail(EINVAL, "Failed to {}: SQL_NEED_DATA: {}", message, error);
		return _log.fail(EINVAL, "Failed to {} data: {}", message, error);
	}
	return 0;
}

namespace {
std::string_view operator_to_string(odbc_scheme::Expression::Operator op)
{
	using O = odbc_scheme::Expression::Operator;
	switch (op) {
	case O::EQ: return "=";
	case O::NE: return "!=";
	case O::LT: return "<";
	case O::LE: return "<=";
	case O::GT: return ">";
	case O::GE: return ">=";
	}
	return "UNKNOWN-OPERATOR";
}

template <typename T>
T * lookup(T * list, std::string_view id)
{
	for (auto i = list; i; i = i->next) {
		if (i->name && i->name == id)
			return i;
	}
	return nullptr;
}
}

int ODBC::_post_control(const tll_msg_t *msg, int flags)
{
	if (internal.caps & tll::caps::Output) {
		// Handle begin/commit/rollback
		return 0;
	}

	if (msg->msgid != odbc_scheme::Query::meta_id())
		return _log.fail(EINVAL, "Invalid control message id: {}", msg->msgid);
	if (_select_sql)
		return _log.fail(EINVAL, "Previous query is not finished, can not start new");

	auto query = odbc_scheme::Query::bind(*msg);

	auto it = _messages.find(query.get_message());
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found in scheme", query.get_message());
	auto & select = it->second;

	std::list<std::string> names;
	if (select.with_seq)
		names.push_back(_quoted("_tll_seq"));
	for (auto & f : tll::util::list_wrap(select.message->fields)) {
		if (&f == select.message->pmap)
			continue;
		names.push_back(_quoted(f.name));
	}
	std::list<std::string> where;
	for (auto & e : query.get_expression()) {
		if (!lookup(select.message->fields, e.get_field()))
			return _log.fail(ENOENT, "No such field '{}' in message {}", e.get_field(), select.message->name);
		where.push_back(fmt::format("{} {} ?", _quoted(e.get_field()), operator_to_string(e.get_op())));
	}

	auto str = fmt::format("SELECT {} FROM {}", join(names.begin(), names.end()), _quoted(select.message->name));
	if (where.size())
		str += std::string(" WHERE ") + join(" AND ", where.begin(), where.end());

	_select_sql = _prepare(str);
	if (!_select_sql)
		return _log.fail(EINVAL, "Failed to prepare select statement for table {}: {}", select.message->name, str);

	std::vector<SQLLEN> param;
	param.resize(query.get_expression().size());
	auto idx = 0;

	for (auto & e : query.get_expression()) {
		auto value = e.get_value();
		_log.info("Bind expression field {} ({})", e.get_field(), idx + 1);
		switch (value.union_type()) {
		case value.index_i:
			SQLBindParam(_select_sql, idx + 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) value.view().view(1).data(), &param[idx]);
			break;
		case value.index_f:
			SQLBindParam(_select_sql, idx + 1, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (SQLPOINTER) value.view().view(1).data(), &param[idx]);
			break;
		case value.index_s: {
			auto s = value.unchecked_s();
			param[idx] = s.size();
			SQLBindParam(_select_sql, idx + 1, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) s.data(), &param[idx]);
			break;
		}
		}
		idx++;
	}

	if (auto r = _execute(_select_sql, "select"); r)
		return r;

	_select = &select;

	auto view = tll::make_view(_buf);
	_buf.resize(select.message->size);
	_buf.reserve(65536);

	idx = 1;
	if (_select->with_seq) {
		if (auto r = SQLBindCol(_select_sql, idx++, SQL_C_SBIGINT, &_msg.seq, sizeof(_msg.seq), &_seq_param); !SQL_SUCCEEDED(r))
			return _log.fail(EINVAL, "Failed to bind seq column: {}", odbcerror(_select_sql));
	}
	for (auto & c : select.convert) {
		if (sql_column(_select_sql, c, idx++, c.field, view.view(c.field->offset)))
			return _log.fail(EINVAL, "Failed to bind field {} column: {}", c.field->name, odbcerror(_select_sql));
	}

	_update_dcaps(dcaps::Process | dcaps::Pending);
	return 0;
}

int ODBC::_process(long timeout, int flags)
{
	if (!_select)
		return _log.fail(EINVAL, "No active select statement");

	auto r = SQLFetch(_select_sql);
	if (!SQL_SUCCEEDED(r)) {
		auto error = odbcerror(_select_sql);
		_select = nullptr;
		SQLCloseCursor(_select_sql);
		_select_sql.reset();
		if (r == SQL_NO_DATA) {
			_log.debug("End of data");
			_update_dcaps(0, dcaps::Process | dcaps::Pending);
			tll_msg_t msg = { TLL_MESSAGE_CONTROL };
			msg.msgid = odbc_scheme::EndOfData::meta_id();
			_callback(&msg);
			return 0;
		}
		if (_sqlstate == "08S01")
			return state_fail(EINVAL, "Failed to fetch data: {}", error);
		return _log.fail(EINVAL, "Failed to fetch data: {}", error);
	}

	auto view = tll::make_view(_buf);
	_buf.resize(_select->message->size);

	auto pmap = _select->message->pmap;
	if (pmap)
		memset(view.view(pmap->offset).data(), 0, pmap->size);
	for (auto & c : _select->convert) {
		if (c.param == SQL_NULL_DATA) {
			memset(view.view(c.field->offset).data(), 0, c.field->size);
			continue;
		}
		if (pmap && c.field->index >= 0)
			tll_scheme_pmap_set(view.view(pmap->offset).data(), c.field->index);

		if (c.type == Prepared::Convert::None)
			continue;
		auto data = view.view(c.field->offset);
		if (c.type == Prepared::Convert::String) {
			auto size = c.param;

			tll::scheme::generic_offset_ptr_t ptr = {};
			ptr.offset = data.size();
			if (size == 0) {
				ptr.offset = 0;
				ptr.size = 0;
				ptr.entity = 0;
				tll::scheme::write_pointer(c.field, data, ptr);
				continue;
			}

			ptr.size = size + 1;
			ptr.entity = 1;
			tll::scheme::write_pointer(c.field, data, ptr);
			auto fview = data.view(ptr.offset);
			fview.resize(ptr.size);
			memcpy(fview.data(), c.string, size);
			*fview.view(size).template dataT<char>() = '\0';
		} else if (c.type == Prepared::Convert::Numeric) {
			auto & n = c.numeric;
			tll::util::Decimal128::Unpacked u128;
			u128.exponent = -n.scale;
			u128.sign = n.sign ? 0 : 1;
			for (auto i = 0u; i < sizeof(n.val); i++)
				_log.debug("val[{}] == {}", i, n.val[i]);
			memcpy(&u128.mantissa, n.val, sizeof(u128.mantissa));

			_log.debug("Decimal: sign {}, prec {}, scale {} {} {} ", n.sign, n.precision, n.scale, u128.mantissa.hi, u128.mantissa.lo);
			data.dataT<tll::util::Decimal128>()->pack(u128);
		} else if (c.type == Prepared::Convert::Timestamp) {
			using tll::scheme::Field;
			switch (c.field->type) {
			case Field::Int8: if (auto r = write_time<int8_t>(_log, c, data); r) return r; break;
			case Field::Int16: if (auto r = write_time<int16_t>(_log, c, data); r) return r; break;
			case Field::Int32: if (auto r = write_time<int32_t>(_log, c, data); r) return r; break;
			case Field::Int64: if (auto r = write_time<int64_t>(_log, c, data); r) return r; break;
			case Field::UInt8: if (auto r = write_time<uint8_t>(_log, c, data); r) return r; break;
			case Field::UInt16: if (auto r = write_time<uint16_t>(_log, c, data); r) return r; break;
			case Field::UInt32: if (auto r = write_time<uint32_t>(_log, c, data); r) return r; break;
			case Field::UInt64: if (auto r = write_time<uint64_t>(_log, c, data); r) return r; break;
			case Field::Double: if (auto r = write_time<double>(_log, c, data); r) return r; break;
			default:
				return _log.fail(EINVAL, "Invalid field type for timestamp: {}", c.field->type);
			}
		}
	}

	_msg.msgid = _select->message->msgid;
	_msg.data = _buf.data();
	_msg.size = _buf.size();

	_callback_data(&_msg);
	return 0;
}

TLL_DEFINE_IMPL(HeartBeat);
TLL_DEFINE_IMPL(ODBC);

TLL_DEFINE_MODULE(ODBC, HeartBeat);
