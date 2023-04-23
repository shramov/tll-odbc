#include <tll/channel/base.h>
#include <tll/channel/module.h>

#include <tll/scheme/util.h>
#include <tll/util/listiter.h>
#include <tll/util/memoryview.h>
#include <tll/util/decimal128.h>

#include <sql.h>
#include <sqlext.h>

#include "odbc-scheme.h"

using namespace tll;

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

	void * release() { void * tmp = *this; ptr.reset(); return tmp; }
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
		enum Type { None, ByteString, String, Numeric } type = None;
		const tll::scheme::Field * field;
		SQLLEN param;
		union {
			int64_t integer;
			char * string;
			SQL_NUMERIC_STRUCT numeric;
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

template <typename Buf>
int sql_bind(SQLHSTMT sql, Prepared::Convert &convert, int idx, const tll::scheme::Field *field, const Buf &data)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
		return SQLBindParam(sql, idx, SQL_C_STINYINT, SQL_SMALLINT, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::Int16:
		return SQLBindParam(sql, idx, SQL_C_SSHORT, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::Int32:
		return SQLBindParam(sql, idx, SQL_C_SLONG, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::Int64:
		return SQLBindParam(sql, idx, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::UInt8:
		return SQLBindParam(sql, idx, SQL_C_UTINYINT, SQL_SMALLINT, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::UInt16:
		return SQLBindParam(sql, idx, SQL_C_USHORT, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &convert.param);
	case Field::UInt32:
		return SQLBindParam(sql, idx, SQL_C_ULONG, SQL_BIGINT, 0, 0, (SQLPOINTER) data.data(), &convert.param);

	case Field::Double:
		return SQLBindParam(sql, idx, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (SQLPOINTER) data.data(), &convert.param);

	case Field::UInt64:
		return SQL_ERROR;

	case Field::Decimal128: {
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
		if (field->sub_type == Field::ByteString) {
			auto str = data.template dataT<char>();
			convert.param = strnlen(str, field->size);
			return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) str, &convert.param);
		}
		return SQL_ERROR;

	case Field::Message:
		return SQL_ERROR;
	case Field::Array:
		return SQL_ERROR;
	case Field::Pointer:
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			auto ptr = tll::scheme::read_pointer(field, data);
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
		return SQLBindCol(sql, idx, SQL_C_NUMERIC, (SQLPOINTER) &convert.numeric, sizeof(SQL_NUMERIC_STRUCT), &convert.param);

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
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			return SQLBindCol(sql, idx, SQL_C_CHAR, convert.string, 1024, &convert.param);
		}
		return SQL_ERROR;
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

	std::map<int, Prepared> _messages;

	SQLLEN _seq_param;
	tll_msg_t _msg = {};

	enum class Index { No, Yes, Unique } _seq_index = Index::Unique;
	enum class Create { No, Checked, Always } _create_mode = Create::Checked;
	enum class Quotes { SQLite, PSQL, Sybase } _quotes = Quotes::PSQL;
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

	std::string _quoted(std::string_view name) // Can only be used for table/field names, no escaping performed
	{
		switch (_quotes) {
		case Quotes::SQLite: return fmt::format("`{}`", name);
		case Quotes::PSQL: return fmt::format("\"{}\"", name);
		case Quotes::Sybase: return fmt::format("[{}]", name);
		}
		return std::string(name);
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
			if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
				break;
			if (buf.size() < (size_t) len) {
				buf.resize(len + 1);
				len = 0;
				r = SQLGetDiagRec(type, handle, i, sqlstate.dataT<SQLCHAR>(), &native, buf.dataT<SQLCHAR>(), buf.size(), &len);
			}
			if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
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

	_create_mode = reader.getT("create-mode", Create::Checked, {{"no", Create::No}, {"checked", Create::Checked}, {"always", Create::Always}});
	_quotes = reader.getT("quote-mode", Quotes::PSQL, {{"sqlite", Quotes::SQLite}, {"psql", Quotes::PSQL}, {"sybase", Quotes::Sybase}});
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

int ODBC::_open(const ConstConfig &s)
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
                               (SQLCHAR *) buf, sizeof(buf), &buflen, SQL_DRIVER_NOPROMPT); r != SQL_SUCCESS) {
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
				return _log.fail(EINVAL, "Ouput message {} was not prepared", m.output_message->name);
			m.output = &it->second;
		}
		auto ibuf = _string_buffers.begin();
		auto i = 0;
		for (auto & f : tll::util::list_wrap(m.message->fields)) {
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
		auto options = f.options;
		if (f.type == f.Pointer)
			options = f.type_ptr->options;

		auto t = sql_type(&f);
		if (!t)
			return _log.fail(EINVAL, "Message {} field {}: {}", msg->name, f.name, t.error());

		auto otype = tll::getter::get(options, "sql.column-type").value_or(*t);
		fields.push_back(fmt::format("{} {} NOT NULL", _quoted(f.name), otype));

		auto pkey = tll::getter::getT(options, "sql.primary-key", false);

		if (!pkey)
			_log.warning("Invalid primary-key option: {}", pkey.error());
		else if (*pkey) {
			_log.debug("Field {} is primary key", f.name);
			fields.back() += " PRIMARY KEY";
		}
	}

	sql = _prepare(fmt::format("CREATE TABLE {}{} ({})", _if_not_exists(), _quoted(table), join(fields.begin(), fields.end())));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare CREATE statement");

	if (auto r = SQLExecute(sql); r != SQL_SUCCESS)
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

	for (auto & f : tll::util::list_wrap(msg->fields))
		names.push_back(_quoted(f.name));

	enum Template { None, Insert, Function, Procedure };
	auto tmpl = reader.getT("sql.template", Insert, {{"none", None}, {"insert", Insert}, {"function", Function}, {"procedure", Procedure}});
	auto query = reader.getT("sql.query", std::string());
	auto output = reader.getT("sql.output", std::string());

	if (query.size())
		tmpl = None;

	auto create = reader.getT("sql.create", tmpl == Insert);

	if (!reader)
		return _log.fail(EINVAL, "Failed to read SQL options from message '{}': {}", msg->name, reader.error());

	const tll::scheme::Message * outmsg = nullptr;
	if (output.size()) {
		outmsg = _scheme->lookup(output);
		if (!outmsg)
			return _log.fail(EINVAL, "Output message '{}' for query '{}' not found", output, msg->name);
	}

	switch (tmpl) {
	case None:
		break;
	case Insert:
		query = fmt::format("INSERT INTO {}({}) VALUES ", _quoted(table), join(names.begin(), names.end()));
		for (auto & i : names)
			i = "?";
		query += fmt::format("({})", join(names.begin(), names.end()));
		break;
	case Function: {
		if (!outmsg)
			return _log.fail(EINVAL, "Function template '{}' without output message", msg->name);
		std::list<std::string> outnames;
		for (auto & f : tll::util::list_wrap(outmsg->fields))
			outnames.push_back(_quoted(f.name));
		for (auto & i : names)
			i = "?";
		if (_function_mode == Function::Fields)
			query = fmt::format("SELECT {} FROM", join(outnames.begin(), outnames.end()));
		else
			query = "SELECT";
		query += fmt::format(" {}({})", _quoted(table), join(names.begin(), names.end()));
		break;
	}
	case Procedure:
		for (auto & i : names)
			i = "?";
		query = fmt::format("CALL {}({})", _quoted(table), join(names.begin(), names.end()));
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

	if (auto r = SQLExecute(sql); r != SQL_SUCCESS)
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

	SQLFreeStmt(insert.sql, SQL_RESET_PARAMS);

	auto view = tll::make_view(*msg);

	int idx = 1;
	if (insert.with_seq) {
		if (auto r = SQLBindParam(insert.sql, idx++, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) &msg->seq, &_seq_param); r != SQL_SUCCESS)
			return _log.fail(EINVAL, "Failed to bind seq: {}", odbcerror(insert.sql));
	}
	for (auto & c : insert.convert) {
		if (sql_bind(insert.sql, c, idx++, c.field, view.view(c.field->offset)))
			return _log.fail(EINVAL, "Failed to bind field {}: {}", c.field->name, odbcerror(insert.sql));
	}
	if (auto r = SQLExecute(insert.sql); r != SQL_SUCCESS) {
		if (r == SQL_NEED_DATA)
			return _log.fail(EINVAL, "Failed to insert: SQL_NEED_DATA: {}", odbcerror(insert.sql));
		return _log.fail(EINVAL, "Failed to insert data: {}", odbcerror(insert.sql));
	}

	if (!insert.output)
		return 0;

	{
		_select_sql = insert.sql;
		_select = insert.output;

		auto view = tll::make_view(_buf);
		_buf.resize(_select->message->size);
		_buf.reserve(65536);

		idx = 1;
		if (_select->with_seq) {
			if (auto r = SQLBindCol(_select_sql, idx++, SQL_C_SBIGINT, &_msg.seq, sizeof(_msg.seq), &_seq_param); r != SQL_SUCCESS)
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
	if (internal.caps & caps::Output) {
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
	for (auto & f : tll::util::list_wrap(select.message->fields))
		names.push_back(_quoted(f.name));
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

	if (auto r = SQLExecute(_select_sql); r != SQL_SUCCESS) {
		if (r == SQL_NEED_DATA)
			return _log.fail(EINVAL, "Failed to select data: SQL_NEED_DATA: {}", odbcerror(_select_sql));
		return _log.fail(EINVAL, "Failed to select data: {}", odbcerror(_select_sql));
	}

	_select = &select;

	auto view = tll::make_view(_buf);
	_buf.resize(select.message->size);
	_buf.reserve(65536);

	idx = 1;
	if (_select->with_seq) {
		if (auto r = SQLBindCol(_select_sql, idx++, SQL_C_SBIGINT, &_msg.seq, sizeof(_msg.seq), &_seq_param); r != SQL_SUCCESS)
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
	if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO) {
		_select = nullptr;
		_select_sql.reset();
		if (r == SQL_NO_DATA) {
			_log.debug("End of data");
			_update_dcaps(0, dcaps::Process | dcaps::Pending);
			tll_msg_t msg = { TLL_MESSAGE_CONTROL };
			msg.msgid = odbc_scheme::EndOfData::meta_id();
			_callback(&msg);
			return 0;
		}
		return _log.fail(EINVAL, "Failed to fetch data: {}", odbcerror(_select_sql));
	}

	auto view = tll::make_view(_buf);
	_buf.resize(_select->message->size);

	auto i = 0u;
	if (_select->with_seq)
		i++;
	for (auto & c : _select->convert) {
		i++;
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
		}
	}

	_msg.msgid = _select->message->msgid;
	_msg.data = _buf.data();
	_msg.size = _buf.size();

	_callback_data(&_msg);
	return 0;
}

TLL_DEFINE_IMPL(ODBC);

TLL_DEFINE_MODULE(ODBC);
