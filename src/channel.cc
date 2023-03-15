#include <tll/channel/base.h>
#include <tll/channel/module.h>

#include <tll/scheme/util.h>
#include <tll/util/listiter.h>
#include <tll/util/memoryview.h>

#include <sql.h>
#include <sqlext.h>

#include "odbc-scheme.h"

using namespace tll;

template <SQLSMALLINT Type>
struct SQLHandle
{
	void * ptr = nullptr;
	~SQLHandle() { reset(); }

	SQLHandle() = default;
	SQLHandle(const SQLHandle &) = delete;
	SQLHandle(SQLHandle && rhs) { std::swap(ptr, rhs.ptr); }

	void reset(void * v = nullptr)
	{
		if (ptr)
			SQLFreeHandle(Type, ptr);
		ptr = v;
	}

	operator void * () { return ptr; }
	operator const void * () const { return ptr; }

	void * release() { void * tmp = ptr; ptr = nullptr; return tmp; }
};

using query_ptr_t = SQLHandle<SQL_HANDLE_STMT>;

struct Prepared
{
	Prepared(query_ptr_t && ptr) : sql(std::move(ptr)) {}
	query_ptr_t sql;
	const tll::scheme::Message * message = nullptr;
	std::vector<SQLLEN> param;
	union Convert {
		int64_t i;
		double d;
		char * c;
	};
	std::vector<Convert> convert;
};

namespace {
bool is_offset_string(const tll::scheme::Field * f)
{
	using tll::scheme::Field;
	return f->type == Field::Pointer && f->type_ptr->type == Field::Int8 && f->sub_type == Field::ByteString;
}

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
	case Field::UInt32:
	case Field::Int64:
		return "INTEGER";
	case Field::UInt64:
		return tll::error("UInt64 not supported");

	case Field::Double:
		return "REAL";

	case Field::Decimal128:
		return tll::error("Decimal128 not supported yet");

	case Field::Bytes:
		if (field->sub_type == Field::ByteString)
			return "VARCHAR";
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
int sql_bind(SQLHSTMT sql, std::vector<SQLLEN> param, int idx, const tll::scheme::Field *field, const Buf &data)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
		return SQLBindParam(sql, idx, SQL_C_STINYINT, SQL_SMALLINT, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::Int16:
		return SQLBindParam(sql, idx, SQL_C_SSHORT, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::Int32:
		return SQLBindParam(sql, idx, SQL_C_SLONG, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::Int64:
		return SQLBindParam(sql, idx, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::UInt8:
		return SQLBindParam(sql, idx, SQL_C_UTINYINT, SQL_SMALLINT, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::UInt16:
		return SQLBindParam(sql, idx, SQL_C_USHORT, SQL_INTEGER, 0, 0, (SQLPOINTER) data.data(), &param[idx]);
	case Field::UInt32:
		return SQLBindParam(sql, idx, SQL_C_ULONG, SQL_BIGINT, 0, 0, (SQLPOINTER) data.data(), &param[idx]);

	case Field::Double:
		return SQLBindParam(sql, idx, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, (SQLPOINTER) data.data(), &param[idx]);

	case Field::UInt64:
	case Field::Decimal128:
		return SQL_ERROR;

	case Field::Bytes:
		if (field->sub_type == Field::ByteString) {
			auto str = data.template dataT<char>();
			param[idx] = strnlen(str, field->size);
			return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) str, &param[idx]);
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
				return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) "", &param[idx]);
			param[idx] = ptr->size - 1;
			return SQLBindParam(sql, idx, SQL_C_CHAR, SQL_VARCHAR, 0, 0, (SQLPOINTER) data.view(ptr->offset).template dataT<char>(), &param[idx]);
		}
		return SQL_ERROR;
	case Field::Union:
		return SQL_ERROR;
	}
	return SQL_ERROR;
}

template <typename Buf>
int sql_column(SQLHSTMT sql, Prepared &prepared, int idx, const tll::scheme::Field *field, Buf data)
{
	using tll::scheme::Field;
	switch (field->type) {
	case Field::Int8:
		return SQLBindCol(sql, idx, SQL_C_STINYINT, data.data(), sizeof(int8_t), &prepared.param[idx]);
	case Field::Int16:
		return SQLBindCol(sql, idx, SQL_C_SSHORT, data.data(), sizeof(int16_t), &prepared.param[idx]);
	case Field::Int32:
		return SQLBindCol(sql, idx, SQL_C_SLONG, data.data(), sizeof(int32_t), &prepared.param[idx]);
	case Field::Int64:
		return SQLBindCol(sql, idx, SQL_C_SBIGINT, data.data(), sizeof(int64_t), &prepared.param[idx]);
	case Field::UInt8:
		return SQLBindCol(sql, idx, SQL_C_UTINYINT, data.data(), sizeof(uint8_t), &prepared.param[idx]);
	case Field::UInt16:
		return SQLBindCol(sql, idx, SQL_C_USHORT, data.data(), sizeof(uint16_t), &prepared.param[idx]);
	case Field::UInt32:
		return SQLBindCol(sql, idx, SQL_C_ULONG, data.data(), sizeof(uint32_t), &prepared.param[idx]);

	case Field::Double:
		return SQLBindCol(sql, idx, SQL_C_DOUBLE, data.data(), sizeof(double), &prepared.param[idx]);

	case Field::UInt64:
	case Field::Decimal128:
		return SQL_ERROR;

	case Field::Bytes:
		if (field->sub_type == Field::ByteString) {
			return SQLBindCol(sql, idx, SQL_C_CHAR, data.data(), field->size, &prepared.param[idx]);
		}
		return SQL_ERROR;

	case Field::Message:
		return SQL_ERROR;
	case Field::Array:
		return SQL_ERROR;
	case Field::Pointer:
		if (field->type_ptr->type == Field::Int8 && field->sub_type == Field::ByteString) {
			return SQLBindCol(sql, idx, SQL_C_CHAR, prepared.convert[idx].c, 1024, &prepared.param[idx]);
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
	std::vector<std::vector<char>> string_buffers;

	std::map<int, Prepared> _messages;

	enum class Index { No, Yes, Unique } _seq_index = Index::Unique;

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
	int _create_insert(std::string_view table, const tll::scheme::Message *);
	int _create_select(std::string_view table, const tll::scheme::Message *);
	int _create_index(const std::string_view &name, std::string_view key, bool unique);

	SQLHSTMT _prepare(const std::string_view query)
	{
		_log.debug("Prepare SQL statement:\n\t{}", query);
		query_ptr_t sql;
		if (auto r = SQLAllocHandle(SQL_HANDLE_STMT, _db, &sql.ptr); r != SQL_SUCCESS)
			return _log.fail(nullptr, "Failed to allocate statement: {}\n\t{}", r, query);
		if (auto r = SQLPrepare(sql, (SQLCHAR *) query.data(), query.size()); r != SQL_SUCCESS)
			return _log.fail(nullptr, "Failed to prepare statement: {}\n\t{}", r, query);
		return sql.release();
	}
};

int ODBC::_init(const Channel::Url &url, Channel * master)
{
	auto reader = channel_props_reader(url);
	auto host = url.host();
	if (!host.size())
		return _log.fail(EINVAL, "Empty database name");
	_settings = fmt::format("DSN={}", host);

	if (!reader)
		return _log.fail(EINVAL, "Invalid url: {}", reader.error());

	if ((internal.caps & caps::InOut) == 0) // Defaults to input
		internal.caps |= caps::Input;
	if ((internal.caps & caps::InOut) == caps::InOut)
		return _log.fail(EINVAL, "odbc:// can be either read-only or write-only, need proper dir in parameters");

	_scheme_control.reset(context().scheme_load(odbc_scheme::scheme_string));
	if (!_scheme_control.get())
		return _log.fail(EINVAL, "Failed to load odbc control scheme");

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
		return _log.fail(EINVAL, "Failed to request ODBCv3: {}", r);

	SQLHDBC hdbc = nullptr;
	if (auto r = SQLAllocHandle(SQL_HANDLE_DBC, _env, &hdbc); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to allocate ODBC Connection: {}", r);
	_db.reset(hdbc);

	char buf[SQL_MAX_OPTION_STRING_LENGTH];
	SQLSMALLINT buflen = sizeof(buf);
	if (auto r = SQLDriverConnect (_db, nullptr, (SQLCHAR *) _settings.data(), _settings.size(),
                               (SQLCHAR *) buf, sizeof(buf), &buflen, SQL_DRIVER_NOPROMPT); r != SQL_SUCCESS) {
		return _log.fail(EINVAL, "Failed to connect to {}", _settings);
	}
	_log.info("Connection string: {}", buf); //std::string_view(buf, buflen));

	for (auto & m : tll::util::list_wrap(_scheme->messages)) {
		if (m.msgid == 0) {
			_log.debug("Message {} has no msgid, skip table check", m.name);
			continue;
		}

		auto table = tll::getter::get(m.options, "sql.table").value_or(std::string_view(m.name));

		if (internal.caps & caps::Output) {
			if (_create_table(table, &m))
				return _log.fail(EINVAL, "Failed to create table '{}' for '{}'", table, m.name);
			if (_create_insert(table, &m))
				return _log.fail(EINVAL, "Failed to prepare SQL statement for '{}'", m.name);
		} else {
			if (_create_select(table, &m))
				return _log.fail(EINVAL, "Failed to prepare SQL statement for '{}'", m.name);
			auto table_name = s.get("table");
			//_update_dcaps(dcaps::Process | dcaps::Pending);
		}
	}

	if (internal.caps & caps::Input) {
		auto strings = 0u;
		for (auto & [_, m] : _messages) {
			auto i = 0u;
			for (auto & f : tll::util::list_wrap(m.message->fields)) {
				if (is_offset_string(&f))
					i++;
			}
			strings = std::max(strings, i);
		}
		_log.info("Need {} string buffers", strings);
		string_buffers.resize(strings);
		for (auto & [_, m] : _messages) {
			auto i = 0u, j = 1u;
			for (auto & f : tll::util::list_wrap(m.message->fields)) {
				j++;
				if (is_offset_string(&f)) {
					string_buffers[i].resize(1024);
					m.convert[j].c = string_buffers[i].data();
					//_log.debug("Bind field {} to buffer {} (index {}): {}", f.name, i, j, (void *) m.convert[j].c);
					i++;
				}
			}
		}
	}

	return 0;
}

int ODBC::_close()
{
	_messages.clear();
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

	fields.push_back("`_tll_seq` INTEGER");
	for (auto & f : tll::util::list_wrap(msg->fields)) {
		auto t = sql_type(&f);
		if (!t)
			return _log.fail(EINVAL, "Message {} field {}: {}", msg->name, f.name, t.error());
		fields.push_back(fmt::format("`{}` {} NOT NULL", f.name, *t));

		auto pkey = tll::getter::getT(f.options, "sql.primary-key", false);
		if (f.type == f.Pointer)
			pkey = tll::getter::getT(f.type_ptr->options, "sql.primary-key", false);

		if (!pkey)
			_log.warning("Invalid primary-key option: {}", pkey.error());
		else if (*pkey) {
			_log.debug("Field {} is primary key", f.name);
			fields.back() += " PRIMARY KEY";
		}
	}

	sql.reset(_prepare(fmt::format("CREATE TABLE IF NOT EXISTS `{}` ({})", table, join(fields.begin(), fields.end()))));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare CREATE statement");

	if (auto r = SQLExecute(sql); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to create table '{}'", table);

	{
		auto index = tll::getter::getT(msg->options, "sql.index", _seq_index, {{"no", Index::No}, {"yes", Index::Yes}, {"unique", Index::Unique}});
		if (!index) {
			_log.warning("Invalid sql.index option for {}: {}", msg->name, index.error());
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

int ODBC::_create_insert(std::string_view table, const tll::scheme::Message *msg)
{
	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(msg->fields))
		names.push_back(fmt::format("`{}`", f.name));

	std::string_view operation = "INSERT";
	//if (_replace)
	//	operation = "REPLACE";
	auto query = fmt::format("{} INTO `{}`({}) VALUES ", operation, table, join(names.begin(), names.end()));
	for (auto & i : names)
		i = "?";
	query += fmt::format("({})", join(names.begin(), names.end()));

	query_ptr_t sql;

	sql.reset(_prepare(query));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare insert statement for table {}: {}", table, query);

	auto it = _messages.emplace(msg->msgid, std::move(sql)).first;
	it->second.message = msg;
	it->second.param.resize(names.size() + 1);
	it->second.convert.resize(names.size() + 1);

	return 0;
}

int ODBC::_create_select(std::string_view table, const tll::scheme::Message *msg)
{
	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(msg->fields))
		names.push_back(fmt::format("`{}`", f.name));

	std::string_view operation = "SELECT";
	//if (_replace)
	//	operation = "REPLACE";
	auto query = fmt::format("{} {} FROM {}", operation, join(names.begin(), names.end()), table);
	//for (auto & i : names)
	//	i = "?";
	//insert += fmt::format("({})", join(names.begin(), names.end()));

	query_ptr_t sql;

	sql.reset(_prepare(query));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare select statement for table {}: {}", table, query);

	auto it = _messages.emplace(msg->msgid, std::move(sql)).first;
	it->second.message = msg;
	it->second.param.resize(names.size() + 1);
	it->second.convert.resize(names.size() + 1);

	return 0;
}

int ODBC::_create_index(const std::string_view &name, std::string_view key, bool unique)
{
	_log.debug("Create index for {}: key {}", name, key);
	query_ptr_t sql;

	std::string_view ustr = unique ? "UNIQUE " : "";
	auto str = fmt::format("CREATE {} INDEX IF NOT EXISTS `_tll_{}_{}` on `{}`(`{}`)", ustr, name, key, name, key);
	sql.reset(_prepare(str));
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

	if (msg->msgid == 0)
		return _log.fail(EINVAL, "Unable to insert message without msgid");
	auto it = _messages.find(msg->msgid);
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found", msg->msgid);
	auto & insert = it->second;

	SQLFreeStmt(insert.sql, SQL_RESET_PARAMS);

	auto view = tll::make_view(*msg);

	int idx = 1;
	if (auto r = SQLBindParam(insert.sql, idx++, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) &msg->seq, &insert.param[0]); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to bind seq: {}", r);
	for (auto & f : tll::util::list_wrap(insert.message->fields)) {
		if (sql_bind(insert.sql, insert.param, idx++, &f, view.view(f.offset)))
			return _log.fail(EINVAL, "Failed to bind field {}", f.name);
	}
	if (auto r = SQLExecute(insert.sql); r != SQL_SUCCESS) {
		if (r == SQL_NEED_DATA)
			return _log.fail(EINVAL, "Failed to insert: SQL_NEED_DATA");
		return _log.fail(EINVAL, "Failed to insert data: {}", r);
	}
	return 0;
}

namespace {
std::string_view operator_to_string(odbc_scheme::Expression::Operator op)
{
	using O = odbc_scheme::Expression::Operator;
	switch (op) {
	case O::EQ: return "==";
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
	if (internal.dcaps && dcaps::Process)
		return _log.fail(EINVAL, "Previous query is not finished, can not start new");

	auto query = odbc_scheme::Query::bind(*msg);

	auto it = _messages.find(query.get_message());
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found in scheme", query.get_message());
	auto & select = it->second;

	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(select.message->fields))
		names.push_back(fmt::format("`{}`", f.name));
	std::list<std::string> where;
	for (auto & e : query.get_expression()) {
		if (!lookup(select.message->fields, e.get_field()))
			return _log.fail(ENOENT, "No such field '{}' in message {}", e.get_field(), select.message->name);
		where.push_back(fmt::format("`{}` {} ?", e.get_field(), operator_to_string(e.get_op())));
	}

	auto str = fmt::format("SELECT {} FROM {}", join(names.begin(), names.end()), select.message->name);
	if (where.size())
		str += std::string(" WHERE ") + join(" AND ", where.begin(), where.end());

	_select_sql.reset(_prepare(str));
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
			return _log.fail(EINVAL, "Failed to select data: SQL_NEED_DATA");
		return _log.fail(EINVAL, "Failed to select data: {}", r);
	}

	_select = &select;

	auto view = tll::make_view(_buf);
	_buf.resize(select.message->size);
	_buf.reserve(65536);

	idx = 1;
	if (auto r = SQLBindCol(_select_sql, idx++, SQL_C_SBIGINT, &select.convert[0].i, sizeof(int64_t), &select.param[0]); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to bind seq column: {}", r);
	for (auto & f : tll::util::list_wrap(select.message->fields)) {
		if (sql_column(_select_sql, select, idx++, &f, view.view(f.offset)))
			return _log.fail(EINVAL, "Failed to bind field {} column", f.name);
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
			return 0;
		}
		return _log.fail(EINVAL, "Failed to fetch data: {}", r);
	}

	auto view = tll::make_view(_buf);
	_buf.resize(_select->message->size);

	auto i = 1u;
	for (auto & f : tll::util::list_wrap(_select->message->fields)) {
		i++;
		_log.debug("Field {}, param: {}", f.name, _select->param[i]);
		if (!is_offset_string(&f))
			continue;
		_log.debug("String: {}[{}] (index {})", _select->convert[i].c, _select->param[i], i);

		auto size = _select->param[i];
		auto data = view.view(f.offset);

		tll::scheme::generic_offset_ptr_t ptr = {};
		ptr.offset = data.size();
		if (size == 0) {
			ptr.offset = 0;
			ptr.size = 0;
			ptr.entity = 0;
			tll::scheme::write_pointer(&f, data, ptr);
			continue;
		}

		ptr.size = size + 1;
		ptr.entity = 1;
		tll::scheme::write_pointer(&f, data, ptr);
		auto fview = data.view(ptr.offset);
		fview.resize(ptr.size);
		memcpy(fview.data(), _select->convert[i].c, size);
		*fview.view(size).template dataT<char>() = '\0';
	}

	tll_msg_t msg = {};
	msg.msgid = _select->message->msgid;
	msg.seq = _select->convert[0].i;
	msg.data = _buf.data();
	msg.size = _buf.size();

	_callback_data(&msg);
	return 0;
}

TLL_DEFINE_IMPL(ODBC);

TLL_DEFINE_MODULE(ODBC);
