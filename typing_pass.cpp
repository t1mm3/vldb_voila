#include "typing_pass.hpp"
#include "runtime_framework.hpp"
#include "runtime.hpp"
#include "utils.hpp"

const static std::string kStringType("varchar");

static std::string
type_from_minmax(double min, double max)
{
	ASSERT(min <= max);
#define TYPE(typ, utyp, issigned, _) {\
			double dmin = std::numeric_limits<typ>::min(); \
			double dmax = std::numeric_limits<typ>::max(); \
			if (min >= dmin && max <= dmax) { \
				return #typ; \
			} \
		}

	TYPE_EXPAND_VOILA_CAST(TYPE, 0)
#undef TYPE
	return "i128";
}

static ScalarTypeProps
covering_type_from_props(const std::vector<ScalarTypeProps>& ts)
{
	ASSERT(ts.size() > 0);

	bool force_string = false;

	double dmin = ts[0].dmin;
	double dmax = ts[0].dmax;
	force_string |= ts[0].type == "varchar";

	for (size_t i=1; i<ts.size(); i++) {
		dmin = std::min(dmin, ts[i].dmin);
		dmax = std::max(dmax, ts[i].dmax);
		force_string |= ts[0].type == "varchar";
	}

	return {dmin, dmax, force_string ? "varchar" : type_from_minmax(dmin, dmax)};
}

static void
upcast_arguments(Expression& e, const ScalarTypeProps& final)
{
	for (auto& arg : e.args) {
		ASSERT(arg);
		arg->props.type.arity[0] = final;
	}
}

void
TypingPass::on_statement(LolepopCtx& ctx, StmtPtr& stmt)
{
	recurse_statement(ctx, stmt, true);


	switch (stmt->type) {
	case Statement::Type::Assignment:
		{
			const auto& s = *stmt;
			const auto var = var_name(ctx, s.var);
			var_types[var] = s.expr->props.type;
			stmt->props.type = s.expr->props.type;
			var_const[var] = s.expr->props.constant;
			var_scalar[var] = s.expr->props.scalar;
		}
		break;
	case Statement::Type::Emit:
		lolepop_arg = stmt->expr->props.type;
		break;
	case Statement::Type::EffectExpr:
	case Statement::Type::Loop:
	case Statement::Type::Done:
	case Statement::Type::BlendStmt:
	case Statement::Type::MetaStmt:
		break;
	default:
		ASSERT(false  && "Unhandled Statement");
		break;
	}

	recurse_statement(ctx, stmt, false);
}

static std::string
type_signed_to_unsigned(const std::string& s)
{
		// transform signed to unsigned
#define F(tpe, utpe, issigned, _) if (issigned && !s.compare(#tpe)) return #utpe;
	TYPE_EXPAND_VOILA_CAST(F, _)
#undef F
	return s;
}

static TypeProps
predicate_type()
{
	return TypeProps { TypeProps::Category::Predicate, { { std::nan("0"), std::nan("0"), "pred_t"} } };
}

#include "codegen.hpp"

TypeProps
TypingPass::type_function(ExprPtr& expr)
{
	auto& s = *expr;
	const std::string& f = s.fun;

	auto binary_op = [&] (bool unify, auto op) -> TypeProps {
		ASSERT(s.args.size() == 2 && "must be binary");
		auto& a = s.args[0];
		auto& b = s.args[1];

		ASSERT(a->props.type.arity.size() == 1 && "must be scalar");
		ASSERT(b->props.type.arity.size() == 1 && "must be scalar");

		if (unify) {
			auto& at = a->props.type.arity[0];
			auto& bt = b->props.type.arity[0];
			auto cover = covering_type_from_props({at, bt});

			bool cast_a = cover.type.compare(at.type);
			bool cast_b = cover.type.compare(bt.type);

			if (cast_a || cast_b) {
				bool const_a = a->type == Expression::Type::Constant;
				bool const_b = b->type == Expression::Type::Constant;
				ASSERT((const_a || const_b) && "Todo: implement casts without constants");

				auto cast_const = [&] (auto& old) -> ExprPtr {
					auto r = std::make_shared<Const>(old->fun);
					r->props = old->props;
					r->props.type.arity[0] = cover;
					return r;
				};

				auto cast_fun = [&] (auto& old) {
					std::shared_ptr<Expression> r = 
						std::make_shared<Fun>("castT" + cover.type, ExprList {old}, s.pred);
					r->props = old->props;
					r->props.type.arity[0] = cover;
					return r;
				};

				if (cast_a) {
					s.args[0] = const_a ? cast_const(s.args[0]) : cast_fun(s.args[0]);
				}
				if (cast_b) {
					s.args[1] = const_b ? cast_const(s.args[1]) : cast_fun(s.args[1]);
				}
			}
		}

		s.props.scalar = a->props.scalar && b->props.scalar;
		s.props.constant = a->props.constant && b->props.constant;

		return op(a->props.type.arity[0].dmin, a->props.type.arity[0].dmax,
			b->props.type.arity[0].dmin, b->props.type.arity[0].dmax);
	};

	#define ARITH2_INFIX(name, op) if (!f.compare(#name)) { \
		return binary_op(false, [&] (auto alo, auto ahi, auto blo, auto bhi) { \
			double d = alo op blo; \
			double e = ahi op bhi; \
			if (!std::string("sub").compare(#name)) { \
				d = alo op bhi; \
				e = ahi op blo; \
			} \
			const double lo = std::min(d, e); \
			const double hi = std::max(d, e); \
			return TypeProps { TypeProps::Category::Tuple, { {lo, hi, type_from_minmax(lo, hi)} } }; \
		}); \
	}

	#define COMPARE_INFIX(name, op) if (!f.compare(#name)) { \
		return binary_op(true, [&] (auto alo, auto ahi, auto blo, auto bhi) { \
			const double lo = 0; \
			const double hi = 1; \
			(void)alo; (void)ahi; (void)blo; (void)bhi; \
			return TypeProps { TypeProps::Category::Tuple, { {lo, hi, type_from_minmax(lo, hi)} } }; \
		}); \
	}

	#define LOGIC_INFIX(name, op) COMPARE_INFIX(name, op)

	if (s.is_table_op()) {
		auto type_col = [&] (const auto& in_tpe) {
			std::string col;
			size_t res = s.get_table_column_ref(col);
			ASSERT(res == 2 || res == 1);

			TypeProps tpe(in_tpe);

			auto check_it = tbl_col_types.find(col);
			if (check_it != tbl_col_types.end()) {
				auto other = check_it->second;
				if (tpe != other) {
					std::cerr
						<< "Overwriting table column with different type ..." << std::endl
						<< "old:" << std::endl;
					other.write(std::cerr, " ");
					std::cerr
						<< std::endl
						<< "new:" << std::endl;
					tpe.write(std::cerr, " "); 

					std::cerr
						<< "Trying to merge ..." << std::endl;
					tpe.from_cover(tpe, other);
					// ASSERT(tpe == other && "Cannot overwrite table column with different type");

					std::cerr
						<< std::endl
						<< "new:" << std::endl;
					tpe.write(std::cerr, " "); 
				}
			}
			tbl_col_types[col] = tpe;

			LOG_DEBUG("type tableop '%s': %s = '%s' \n", f.c_str(), col.c_str(),
				tpe.arity[0].type.c_str());
			return tpe;
		};

		auto type_col_min_max = [&] (double lo, double hi) -> TypeProps {
			auto t = TypeProps { TypeProps::Category::Tuple,
				{ {lo, hi, type_from_minmax(lo, hi)} } };
			s.args[0]->props.type = t;

			return type_col(t);
		};

		if (!f.compare("bucket_build") || !f.compare("bucket_flush")) {
			return type_col_min_max(0.0, 0.0);
		}

		if (!f.compare("aggr_gconst1")) {
			return type_col_min_max(0.0, 1.0);
		} else if (!f.compare("aggr_count") || !f.compare("aggr_gcount")) {
			return type_col_min_max(0.0, config.max_card);
		} else if (!f.compare("aggr_sum") || !f.compare("aggr_gsum")) {
			size_t data_idx;

			if (!f.compare("aggr_sum")) {
				data_idx = 2;
				ASSERT(s.args.size() == 3 && "must be ternary");
			} else {
				data_idx = 1;
				ASSERT(s.args.size() == 2 && "must be binary");
			}
			auto& a = *s.args[data_idx];
			ASSERT(a.props.type.arity.size() == 1 && "must be scalar");

			return type_col_min_max(
				(double)config.max_card * a.props.type.arity[0].dmin,
				(double)config.max_card * a.props.type.arity[0].dmax);
		} else if (!f.compare("aggr_min") || !f.compare("aggr_max")) {
			ASSERT(s.args.size() == 3 && "must be ternary");
			auto& a = *s.args[2];
			ASSERT(a.props.type.arity.size() == 1 && "must be scalar");

			return type_col_min_max(a.props.type.arity[0].dmin,
				a.props.type.arity[0].dmax);
		} else if (!f.compare("read") || !f.compare("gather")) {
			std::string col;
			size_t res = s.get_table_column_ref(col);
			ASSERT(res == 2);
			ASSERT(col.size() > 0);

			auto it = tbl_col_types.find(col);

			if (it == tbl_col_types.end()) {
				std::cerr << "Cannot find column '" << col << "'" << std::endl;
				ASSERT(false && "Cannot find column");
			}
			return it->second;
		} else if (!f.compare("check") || !f.compare("scatter") || !f.compare("write")) {
			std::string ttbl, tcol;

			size_t res = s.get_table_column_ref(ttbl, tcol);
			ASSERT(res == 2);

			ASSERT(s.args.size() > 2);
			auto c = s.args[2];

			auto r = type_col(c->props.type);
			if (!f.compare("check")) {
				return TypeProps {TypeProps::Category::Tuple,
					{{ 0, 1, "u8" }}}; 				
			} else {
				return r;
			}
		} else if (!f.compare("bucket_lookup") || !f.compare("bucket_next") || !f.compare("bucket_insert") || !f.compare("bucket_link")) {
			return TypeProps {TypeProps::Category::Tuple,
				{{ config.hash_dmin, config.hash_dmax, "u64" }}}; 
		} else {
			std::cerr << "Unknown table op '" << f << "'" << std::endl;
			ASSERT(false && "Unknown table op");
		}

		ASSERT(false && "Unhandled case in table op");

		return TypeProps {};
	}

	if (!f.compare("scan")) {
		std::string ttbl, tcol;

		size_t res = s.get_table_column_ref(ttbl, tcol);
		ASSERT(res == 2);

		auto bt = codegen.base_tables[ttbl];
		ASSERT(bt && "Table does not exist");
		auto col = bt->cols[tcol];
		ASSERT(col && "Column does not exist");

		auto tpe = col->type;
		if (col->dmin >= 0.0) {
			tpe = type_signed_to_unsigned(tpe);
		}
		return TypeProps { TypeProps::Category::Tuple,
			{ { col->dmin, col->dmax, tpe} } };
	}

	{
		const bool is_hash = !f.compare("hash");
		const bool is_rehash = !f.compare("rehash");
		if (is_hash || is_rehash) {
			if (is_hash) {
				ASSERT(s.args.size() == 1);
			}
			if (is_rehash) {
				ASSERT(s.args.size() == 2);	
			}
			return TypeProps { TypeProps::Category::Tuple,
				{{ config.hash_dmin, config.hash_dmax, "u64" }}};
		}
	}

	if (!f.compare("blend_expr")) {
		return s.args[0]->props.type;
	}

	if (!f.compare("sequence")) {
		s.props.constant = true;

		double dmin = 0;
		double dmax = config.vector_size;

		auto& a = *s.args[0];
		ASSERT(a.props.type.arity.size() == 1 && "must be scalar");

		auto& arg = a.props.type.arity[0];

		if (!arg.type.compare("pos_t")) {
			dmin = 0;
			dmax = std::numeric_limits<uint64_t>::max();
		} else {
			dmin += arg.dmin;
			dmax += arg.dmax;			
		}

		return TypeProps {TypeProps::Category::Tuple,
			{{ dmin, dmax, type_from_minmax(dmin, dmax)}}};
	}

	if (!f.compare("print")) {
		return s.args[0]->props.type;
	}
	if (!f.compare("contains")) {
		return TypeProps {TypeProps::Category::Tuple,
				{{ 0, 1, "u8" }}}; 
	}
	if (!f.compare("extract_year")) {
		return TypeProps {TypeProps::Category::Tuple,
				{{ 0, 16000, "u16" }}}; 
	}

	ARITH2_INFIX(add, +)
	ARITH2_INFIX(sub, -)
	ARITH2_INFIX(mul, *)

	COMPARE_INFIX(le, <=)
	COMPARE_INFIX(lt, <)
	COMPARE_INFIX(ge, >=)
	COMPARE_INFIX(gt, >)
	COMPARE_INFIX(eq, ==)
	COMPARE_INFIX(ne, =!)

	LOGIC_INFIX(and, &)
	LOGIC_INFIX(or, |)

	// Tuple append
	if (!f.compare("tappend")) {
		TypeProps r;
		r.category = TypeProps::Category::Tuple;
		for (auto& a : s.args) {
			for (auto& t : a->props.type.arity) {
				r.arity.push_back(t);
			}
		}
		return r;
	}

	// Get tuple
	if (!f.compare("tget")) {
		long long idx = TupleGet::get_idx(s);

		ASSERT(s.args[0]->props.type.arity.size() > idx && "Index must be valid");
		return TypeProps { TypeProps::Category::Tuple, { s.args[0]->props.type.arity[idx] }};
	}

	if (s.is_select()) {
		if (!f.compare("selvalid")) {
			auto& a = *s.args[0];
			s.props.constant = a.props.constant;
			s.props.scalar = a.props.scalar;

			ASSERT(s.props.scalar);
		}
		return predicate_type();
	}

	if (s.is_get_pos()) {
		s.props.scalar = true;
		return TypeProps { TypeProps::Category::Tuple, { { std::nan("0"), std::nan("0"), "Position"} } };
	}

	if (s.is_get_morsel()) {
		s.props.scalar = true;
		return TypeProps { TypeProps::Category::Tuple, { { std::nan("0"), std::nan("0"), "Morsel"} } };	
	}

	std::cerr << "Unknown function '" << s.fun << "'" << std::endl;
	ASSERT(false && "Unknown function");
	return TypeProps { };
}
	
void
TypingPass::on_expression(LolepopCtx& ctx, ExprPtr& expr)
{
	recurse_expression(ctx, expr);

	auto& s = *expr;

	TypeProps& t = s.props.type;

	// only type, if it doesn't have a type
	if (t.category != TypeProps::Category::Unknown) {
#ifdef VERBOSE
		LOG_TRACE("%lu: %p(%s): already typed\n",
			round, &s, s.fun.c_str());
#endif
		return;
	}

	switch (s.type) {
	case Expression::Type::Function:
		t = type_function(expr);
		rules_applied++;
		break;
	case Expression::Type::Constant:
	 	{
			long long cval;
			bool is_cardinal = parse_cardinal(cval, s.fun);

			if (is_cardinal) {
				const double v = cval;
				t = TypeProps {TypeProps::Category::Tuple, {{ v, v, type_from_minmax(v, v) }}};
			} else {
				double v = std::nan("0");
				t = TypeProps {TypeProps::Category::Tuple, {{ v, v, kStringType }}};
			}
			s.props.scalar = true;
			s.props.constant = true;
			rules_applied++;
	 	}
		break;

	case Expression::Type::LoleArg:
		LOG_TRACE("%lu: %p(%s): typing LoleArg\n",
			round, &s, s.fun.c_str());

		t = lolepop_arg;
		// rules_applied++;
		break;

	case Expression::Type::Reference:
		{
			const auto var = var_name(ctx, s.fun);
			auto variable_it = var_types.find(var);
			if (variable_it != var_types.end()) {
				t = variable_it->second;
				s.props.constant = var_const[var];
				s.props.scalar = var_scalar[var];

				rules_applied++;
				break;
			}

#if 0
			// is base column
			std::string ttbl, tcol;

			size_t res = s.get_table_column_ref(ttbl, tcol);
			if (res == 2) {
				auto bt = codegen.base_tables[ttbl];
				if (bt) {
					auto col = bt->cols[tcol];
					if (col) {
						t.arity = { ScalarTypeProps {0.0, 0.0, col->type} };
						break;
					}
				}
			}
#endif
			const auto& tcol = s.fun;
			// try intermediate table
			// printf("%lu: Check column '%s'\n", round, tcol.c_str());
			auto check_it = tbl_col_types.find(tcol);
			if (check_it != tbl_col_types.end()) {
				t = check_it->second;
				ASSERT(t.arity.size() > 0);
				// printf("%p\n", &s);
				rules_applied++;
				break;
			}
		}
		LOG_DEBUG("%lu: Failed to type Reference '%s'@%p\n",
			round, s.fun.c_str(), &s);
		break;
	case Expression::Type::LolePred:
		LOG_TRACE("%lu: %p(%s): typing LolePred\n",
			round, &s, s.fun.c_str());	

		t = predicate_type();
		break;

	default:
		ASSERT(false && "Unhandled case");
		break;

	}
}

void
TypingPass::operator()(Program& p)
{
	round = 0;
	do {
		reset();

		rules_applied = 0;
		round++;
		pipeline_id = 0;
		on_program(p);

		LOG_TRACE("%lu: rules_applied %lu\n", round, rules_applied);

		ASSERT(round < 100);
	} while (rules_applied > 0);
}

void
TypingPass::on_lolepop(Pipeline& p, Lolepop& l)
{
	LOG_TRACE("<LOLEPOP %s>\n", l.name.c_str());
	recurse_lolepop(p, l);
	LOG_TRACE("</LOLEPOP %s>\n", l.name.c_str());
}

void
TypingPass::on_pipeline(Pipeline& p)
{
	LOG_TRACE("<PIPELINE id=%d>\n", pipeline_id);
	recurse_pipeline(p);
	LOG_TRACE("</PIPELINE id=%d>\n", pipeline_id);
	pipeline_id++;
}