#include <tll/channel/base.h>
#include <tll/channel/module.h>

#include <tll/scheme/util.h>
#include <tll/util/listiter.h>
#include <tll/util/memoryview.h>

#include <sql.h>
#include <sqlext.h>

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

class ODBC : public tll::channel::Base<ODBC>
{
	using Base = tll::channel::Base<ODBC>;

	SQLHandle<SQL_HANDLE_ENV> _env;
	SQLHandle<SQL_HANDLE_DBC> _db;

	std::string _settings;

	struct Insert
	{
		Insert(query_ptr_t && ptr) : sql(std::move(ptr)) {}
		query_ptr_t sql;
		std::vector<SQLLEN> param;
	};

	std::map<int, std::pair<const tll::scheme::Message *, Insert>> _messages;

	enum class Index { No, Yes, Unique } _seq_index = Index::Unique;

 public:
//	static constexpr std::string_view sqlite_control_scheme() { return sqlite_scheme::scheme; }

	static constexpr std::string_view channel_protocol() { return "odbc"; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();
	//void _destroy();

	int _post(const tll_msg_t *msg, int flags);
	//int _process(long timeout, int flags);

 private:
	int _create_table(std::string_view table, const tll::scheme::Message *);
	int _create_statement(std::string_view table, const tll::scheme::Message *);
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

	auto table_name = s.get("table");

	for (auto & m : tll::util::list_wrap(_scheme->messages)) {
		if (m.msgid == 0) {
			_log.debug("Message {} has no msgid, skip table check", m.name);
			continue;
		}

		auto table = tll::getter::get(m.options, "sql.table").value_or(std::string_view(m.name));

		if (_create_table(table, &m))
			return _log.fail(EINVAL, "Failed to create table '{}' for '{}'", table, m.name);
		if (_create_statement(table, &m))
			return _log.fail(EINVAL, "Failed to prepare SQL statement for '{}'", m.name);
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

int ODBC::_create_statement(std::string_view table, const tll::scheme::Message *msg)
{
	std::list<std::string> names;
	names.push_back("`_tll_seq`");
	for (auto & f : tll::util::list_wrap(msg->fields))
		names.push_back(fmt::format("`{}`", f.name));

	std::string_view operation = "INSERT";
	//if (_replace)
	//	operation = "REPLACE";
	auto insert = fmt::format("{} INTO `{}`({}) VALUES ", operation, table, join(names.begin(), names.end()));
	for (auto & i : names)
		i = "?";
	insert += fmt::format("({})", join(names.begin(), names.end()));

	query_ptr_t sql;

	sql.reset(_prepare(insert));
	if (!sql)
		return _log.fail(EINVAL, "Failed to prepare insert statement for table {}: {}", table, insert);

	auto it = _messages.emplace(msg->msgid, std::make_pair(msg, std::move(sql))).first;
	it->second.second.param.resize(names.size() + 1);

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
	if (msg->type != TLL_MESSAGE_DATA)
		return 0;

	if (msg->msgid == 0)
		return _log.fail(EINVAL, "Unable to insert message without msgid");
	auto it = _messages.find(msg->msgid);
	if (it == _messages.end())
		return _log.fail(ENOENT, "Message {} not found", msg->msgid);
	auto & [message, insert] = it->second;

	SQLFreeStmt(insert.sql, SQL_RESET_PARAMS);

	auto view = tll::make_view(*msg);

	int idx = 1;
	if (auto r = SQLBindParam(insert.sql, idx++, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (SQLPOINTER) &msg->seq, &insert.param[0]); r != SQL_SUCCESS)
		return _log.fail(EINVAL, "Failed to bind seq: {}", r);
	for (auto & f : tll::util::list_wrap(message->fields)) {
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

TLL_DEFINE_IMPL(ODBC);

TLL_DEFINE_MODULE(ODBC);
