#pragma once

#include <tll/scheme/binder.h>
#include <tll/util/conv.h>

namespace odbc_scheme {

static constexpr std::string_view scheme_string = R"(yamls+gz://eJx1kUFPhDAQhe/8irk12UDCAmsMN3WJF+NmjTfjoSsDaSwtgWIkG/67U9xucTfeXt73Oq+dRqB4gzmwe6yFYgGAKHNYx0HkwINuGmEcSRbkRUt54B+fjqULVny3Hfa90PNMVEPT5yQA2K7FjhvdsRyOZmwpLJS5DecMWazYE4lDYI8FidSKVxIJiSfrbKywTkbi2TrraaLRg6IyV3KnRjt/9nJ4O55uJVgIv52MSm8yNoXgWOVZqYeDxCXsPexNJ1TNpndbWgmU5ak08qOse33iIqVbHzkv5TL0xeWAPmffNfkt7wfsRrf+LP73Qg19Ba/xz+vT5KoM/aedk6vFTy6aC1Xuqi033LVv4uAHLpCokg==)";

struct Begin
{
	static constexpr size_t meta_size() { return 0; }
	static constexpr std::string_view meta_name() { return "Begin"; }
	static constexpr int meta_id() { return 10; }

	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return Begin::meta_size(); }
		static constexpr auto meta_name() { return Begin::meta_name(); }
		static constexpr auto meta_id() { return Begin::meta_id(); }
		void view_resize() { this->_view_resize(meta_size()); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

struct Commit
{
	static constexpr size_t meta_size() { return 0; }
	static constexpr std::string_view meta_name() { return "Commit"; }
	static constexpr int meta_id() { return 20; }

	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return Commit::meta_size(); }
		static constexpr auto meta_name() { return Commit::meta_name(); }
		static constexpr auto meta_id() { return Commit::meta_id(); }
		void view_resize() { this->_view_resize(meta_size()); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

struct Rollback
{
	static constexpr size_t meta_size() { return 0; }
	static constexpr std::string_view meta_name() { return "Rollback"; }
	static constexpr int meta_id() { return 30; }

	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return Rollback::meta_size(); }
		static constexpr auto meta_name() { return Rollback::meta_name(); }
		static constexpr auto meta_id() { return Rollback::meta_id(); }
		void view_resize() { this->_view_resize(meta_size()); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

struct Expression
{
	static constexpr size_t meta_size() { return 18; }
	static constexpr std::string_view meta_name() { return "Expression"; }

	enum class Operator: int8_t
	{
		EQ = 0,
		NE = 1,
		GT = 2,
		GE = 3,
		LT = 4,
		LE = 5,
	};

	template <typename Buf>
	struct Any: public tll::scheme::binder::Union<Buf, int8_t>
	{
		using union_index_type = int8_t;
		using tll::scheme::binder::Union<Buf, union_index_type>::Union;

		static constexpr union_index_type index_i = 0;
		using type_i = int64_t;
		std::optional<int64_t> get_i() const { if (this->union_type() != index_i) return std::nullopt; return unchecked_i(); }
		int64_t unchecked_i() const { return this->template _get_scalar<int64_t>(1); }
		void set_i(const int64_t &v) { this->_set_type(index_i); this->template _set_scalar<int64_t>(1, v); }

		static constexpr union_index_type index_f = 1;
		using type_f = double;
		std::optional<double> get_f() const { if (this->union_type() != index_f) return std::nullopt; return unchecked_f(); }
		double unchecked_f() const { return this->template _get_scalar<double>(1); }
		void set_f(const double &v) { this->_set_type(index_f); this->template _set_scalar<double>(1, v); }

		static constexpr union_index_type index_s = 2;
		using type_s = tll::scheme::binder::String<Buf, tll_scheme_offset_ptr_t>;
		std::optional<std::string_view> get_s() const { if (this->union_type() != index_s) return std::nullopt; return unchecked_s(); }
		std::string_view unchecked_s() const { return this->template _get_string<tll_scheme_offset_ptr_t>(1); }
		void set_s(std::string_view v) { this->_set_type(index_s); this->template _set_string<tll_scheme_offset_ptr_t>(1, v); }
	};


	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return Expression::meta_size(); }
		static constexpr auto meta_name() { return Expression::meta_name(); }
		void view_resize() { this->_view_resize(meta_size()); }

		std::string_view get_field() const { return this->template _get_string<tll_scheme_offset_ptr_t>(0); }
		void set_field(std::string_view v) { return this->template _set_string<tll_scheme_offset_ptr_t>(0, v); }

		using type_op = Operator;
		type_op get_op() const { return this->template _get_scalar<type_op>(8); }
		void set_op(type_op v) { return this->template _set_scalar<type_op>(8, v); }

		using type_value = Any<Buf>;
		const type_value get_value() const { return this->template _get_binder<type_value>(9); }
		type_value get_value() { return this->template _get_binder<type_value>(9); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

struct Query
{
	static constexpr size_t meta_size() { return 12; }
	static constexpr std::string_view meta_name() { return "Query"; }
	static constexpr int meta_id() { return 40; }

	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return Query::meta_size(); }
		static constexpr auto meta_name() { return Query::meta_name(); }
		static constexpr auto meta_id() { return Query::meta_id(); }
		void view_resize() { this->_view_resize(meta_size()); }

		using type_message = int32_t;
		type_message get_message() const { return this->template _get_scalar<type_message>(0); }
		void set_message(type_message v) { return this->template _set_scalar<type_message>(0, v); }

		using type_expression = tll::scheme::binder::List<Buf, Expression::binder_type<Buf>, tll_scheme_offset_ptr_t>;
		const type_expression get_expression() const { return this->template _get_binder<type_expression>(4); }
		type_expression get_expression() { return this->template _get_binder<type_expression>(4); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

struct EndOfData
{
	static constexpr size_t meta_size() { return 0; }
	static constexpr std::string_view meta_name() { return "EndOfData"; }
	static constexpr int meta_id() { return 50; }

	template <typename Buf>
	struct binder_type : public tll::scheme::Binder<Buf>
	{
		using tll::scheme::Binder<Buf>::Binder;

		static constexpr auto meta_size() { return EndOfData::meta_size(); }
		static constexpr auto meta_name() { return EndOfData::meta_name(); }
		static constexpr auto meta_id() { return EndOfData::meta_id(); }
		void view_resize() { this->_view_resize(meta_size()); }
	};

	template <typename Buf>
	static binder_type<Buf> bind(Buf &buf, size_t offset = 0) { return binder_type<Buf>(tll::make_view(buf).view(offset)); }
};

} // namespace odbc_scheme

template <>
struct tll::conv::dump<odbc_scheme::Expression::Operator> : public to_string_from_string_buf<odbc_scheme::Expression::Operator>
{
	template <typename Buf>
	static inline std::string_view to_string_buf(const odbc_scheme::Expression::Operator &v, Buf &buf)
	{
		switch (v) {
		case odbc_scheme::Expression::Operator::EQ: return "EQ";
		case odbc_scheme::Expression::Operator::GE: return "GE";
		case odbc_scheme::Expression::Operator::GT: return "GT";
		case odbc_scheme::Expression::Operator::LE: return "LE";
		case odbc_scheme::Expression::Operator::LT: return "LT";
		case odbc_scheme::Expression::Operator::NE: return "NE";
		default: break;
		}
		return tll::conv::to_string_buf<int8_t, Buf>((int8_t) v, buf);
	}
};
