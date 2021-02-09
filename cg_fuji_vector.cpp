#include "cg_fuji_vector.hpp"
#include "cg_fuji.hpp"
#include "runtime.hpp"
#include "cg_fuji_control.hpp"
#include "utils.hpp"

static const std::string kPredType = "sel_t";

static const std::string kColPrefix = "col_";

static const auto kVarDestScope = clite::Variable::Scope::Local;

#define EOL std::endl

static inline std::string
translate_type(const std::string& t)
{
	if (!t.compare("pred_t")) {
		return "u32";
	}
	if (!t.compare("pos_t")) {
		return "u64";
	}
	return t;
}


VectorDataGen::VectorExpr*
VectorDataGen::get(const ExprPtr& e)
{
	LOG_DEBUG("VectorDataGen::get(%p)\n", e.get());
	ASSERT(e);
	auto p = get_ptr(e);
	if (!p) {
		LOG_ERROR("VectorDataGen::get(%p): Pointer is NULL\n",
			e.get());
		// ASSERT(p);
		return nullptr;
	}
	auto r = dynamic_cast<VectorExpr*>(p.get());
	if (!r) {
		LOG_ERROR("Not castable to VectorDataGen::VectorExpr: this='%s', p='%s'\n",
			get_flavor_name().c_str(), p->src_data_gen.c_str());
		ASSERT(r && "Must be castable to DataGen's type");
	}
	return r;
}


clite::VarPtr
VectorDataGen::expr2get0(const ExprPtr& arg)
{
	auto a = get(arg);
	ASSERT(a);
	return a->var;
}


DataGenExprPtr
VectorDataGen::new_simple_expr(const DataGen::SimpleExprArgs& args)
{
	clite::Factory factory;

	const std::string vid(args.id.empty() ? unique_id() : args.id);
	auto var = get_fragment().new_var(vid, args.type,
		args.scope);


	var->prevent_promotion = true;
	var->dbg_name = args.dbg_name;

	auto r = std::make_shared<VectorExpr>(*this, var, args.predicate,
		"new_simple_expr: " + args.dbg_name, true, false, args.type);

	return r;
}

static void assert_scalar_num(const DataGenExprPtr& a)
{
	auto num = std::dynamic_pointer_cast<VectorDataGen::VectorExpr>(a);
	ASSERT(num)
	ASSERT(num->is_num);
	ASSERT(!num->num);

	ASSERT(num->var);
	LOG_DEBUG("assert_scalar_num: %s\n", num->var->type.c_str());

	if (num->scalar) {	
		ASSERT(str_in_strings(num->var->type, {
			"sel_t"
		}));
	}
}

DataGenExprPtr
VectorDataGen::new_non_selective_predicate()
{
	clite::Builder builder(*m_codegen.get_current_state());
	auto fragment = get_fragment();

	const std::string prefix("FujiAllocatedVector<" + kPredType + "," + std::to_string(m_unroll_factor) + ">");

	auto dest_var = fragment.new_var(unique_id(), prefix,
		kVarDestScope);

	dest_var->prevent_promotion = true;
	auto result = std::make_shared<VectorExpr>(*this, dest_var,
		true, "expr new_non_selective_predicate", false, false, kPredType);

	result->num = std::make_shared<VectorExpr>(*this, const_vector_size(),
		false, "num new_non_selective_predicate", true, true, "sel_t");
	assert_scalar_num(result->num);

	m_codegen.register_reset_var_vec(dest_var);
	// m_codegen.register_reset_var_int(result->num->var);

	dest_var->ctor_body.emplace_back(builder.effect(
		builder.function("IFujiVector::IDENTITY<" + kPredType + ">",
			builder.reference(dest_var),
			builder.reference(result->num->var))));
	return result;
}

DataGenExprPtr
VectorDataGen::clone_expr(const DataGenExprPtr& _a)
{
	auto& frag = get_fragment();

	ASSERT(_a);

	auto a = std::dynamic_pointer_cast<VectorExpr>(_a);
	ASSERT(a);

	auto result = std::make_shared<VectorExpr>(*this,
		frag.clone_var(unique_id(), a->var),
		a->predicate, "clone_expr", a->scalar,
		a->is_num, a->scal_type);

	if (a->predicate && !a->scalar) {
		m_codegen.register_reset_var_vec(result->var);
	}

	if (a->num) {
		assert_scalar_num(a->num);
		result->num = clone_expr(a->num);
	}

	if (!a->scalar) {
		result->var->prevent_promotion = a->var->prevent_promotion;
	}

	result->clone_origin = a.get();
	// result->simple = a->simple;

	return result;
}

void
VectorDataGen::copy_expr(DataGenExprPtr& _res, const DataGenExprPtr& _a)
{
	ASSERT(_res);
	ASSERT(_a);

	auto a = std::dynamic_pointer_cast<VectorExpr>(_a);
	ASSERT(a);
	auto res = std::dynamic_pointer_cast<VectorExpr>(_res);
	ASSERT(res);

	clite::Builder builder(*m_codegen.get_current_state());
	
	builder << builder.comment("<copy_expr>");

	ASSERT(a->var);
	ASSERT(res->var);

	ASSERT(_res.get() != _a.get());

	ASSERT(_res->var.get() != _a->var.get());
	ASSERT(res->var.get() != a->var.get());

	if (a->scalar) {
		builder << builder.assign(res->var, builder.reference(a->var));
	} else {
		builder << builder.effect(builder.function("IFujiVector::SET_FIRST",
			builder.reference(res->var),
			builder.function("IFujiVector::SELF_GET_FIRST", builder.reference(a->var))));
	}
	if (a->num) {
		assert_scalar_num(a->num);
		builder << builder.comment("<copy num>");

		if (!res->num) {
			res->num = clone_expr(a->num);
		}
		copy_expr(res->num, a->num);
		builder << builder.comment("</copy num>");
	}

	builder << builder.comment("</copy_expr>");
}

void
VectorDataGen::gen_expr(ExprPtr& e)
{
	gen(e, false);
}

void
VectorDataGen::gen_pred(ExprPtr& e)
{
	gen(e, true);
}

struct PrimitiveSignature {
	const std::string& name;
	const std::string& result;
	const std::vector<std::string>& types;
};

static std::string
primitive_name(const PrimitiveSignature& sign)
{
	ASSERT(sign.name.size() > 0);
	ASSERT(sign.result.size() > 0);
	ASSERT(sign.types.size() > 0);

	std::ostringstream s;
	s << "vec_" << sign.name << "__" << sign.result << "_";

	for (auto& t : sign.types) {
		s << "_" << t << "_col";
	}

	return s.str();
}



void
VectorDataGen::gen(ExprPtr& e, bool pred)
{
	const auto& res_type0 = translate_type(e->props.type.arity[0].type);

	clite::Factory factory;
	clite::StmtList statements;


	clite::VarPtr dest_var;
	clite::VarPtr dest_num;

	bool match = false;

	bool is_predicate = false;

	auto new_dest = [&] (const std::string& res_type, bool alloc = true) {	
		std::ostringstream vec_type;

		if (alloc) {
			vec_type << "FujiAllocatedVector" << "<" << res_type
				<< "," << m_unroll_factor << ">";
		} else {
			vec_type << "IFujiVector";
		}

		auto ptr = get_fragment().new_var(unique_id(), vec_type.str(),
			clite::Variable::Scope::Local);
		ptr->prevent_promotion = true;

		return ptr;
	};

	if (!match && !e->fun.compare("scan")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		auto index = get(e->args[1]);
		ASSERT(index);

		ASSERT(tbl.size() > 0);
		ASSERT(col.size() > 0);

		auto scan_ptr = get_fragment().new_var(unique_id(), res_type0 + "*",
			clite::Variable::Scope::ThreadWide, false,
			"(" + res_type0 + "*)(thread." + tbl + "->col_" + col + ".get(0))");

		dest_var = new_dest(res_type0);

		clite::ExprPtr ptr = factory.function("ADDRESS_OF", factory.array_access(factory.reference(scan_ptr),
			factory.function("POSITION_OFFSET", factory.reference(index->var))));

		statements.emplace_back(factory.effect(factory.function("IFujiVector::SET_FIRST",
			factory.reference(dest_var), ptr)));

		auto& child = e->args[1];
		auto child_expr = get(child);

		ASSERT(child_expr);
		assert_scalar_num(child_expr->num);
		dest_num = child_expr->num->var;
		ASSERT(dest_num);
		match = true;
	}

	if (!match && e->type == Expression::Function) {
		ASSERT(e->props.type.arity.size() == 1 && "Must be scalar");

		bool table_out = false;
		bool table_in = false;
		bool column = false;
		bool table_op = e->is_table_op(&table_out, &table_in, &column);

		clite::ExprList arg_exprs;
		std::vector<std::string> arg_types;
		std::string tbl;
		std::string col;

		clite::ExprPtr result;

		if (table_op) {
			size_t r;
			if (column) {
				r = e->get_table_column_ref(tbl, col);
				ASSERT(r == 2);
			} else {
				r = e->get_table_column_ref(tbl);
				ASSERT(r == 1);
			}

			if (column && table_in && !table_out) {
				ASSERT(col.size() > 0);

				std::string tpe = e->props.type.arity[0].type;
				if (!e->fun.compare("check")) {
					tpe = e->args[0]->props.type.arity[0].type;
				}
				// ASSERT(col.size() > 0);
				arg_exprs.emplace_back(factory.cast(tpe+"*",
					factory.literal_from_str("&thread." + tbl + "->col_" + col)));
				arg_types.emplace_back(tpe);
			}

			if (column) {
				ASSERT(col.size() > 0);

				auto coldef = get_fragment().new_var(unique_id(),
					"ITable::ColDef*", clite::Variable::Scope::Local);

				coldef->prevent_promotion = true;
				coldef->ctor_body.emplace_back(
					factory.assign(coldef,
						factory.literal_from_str("&thread." + tbl + "->coldef_" + col)));

				arg_exprs.emplace_back(factory.cast("u64*", 
					factory.function("ADDRESS_OF", factory.reference(coldef))));
				arg_types.emplace_back("u64");
			}

			if (!column) {
				if (!str_in_strings(e->fun, {
					"bucket_lookup", "bucket_next", "bucket_insert",
					"bucket_insert_done", "bucket_link", "bucket_build",
					"bucket_flush"
				})) {
					ASSERT(false && "todo");
				}

				ASSERT(arg_exprs.size() == 0);
				arg_exprs.emplace_back(factory.literal_from_str("(u64*)&thread." + tbl));
				arg_types.emplace_back("u64");

				// add Pipeline for thread id
				if (!e->fun.compare("bucket_build")) {
					arg_exprs.emplace_back(factory.literal_from_str("(u64*)&pipeline"));
					arg_types.emplace_back("u64");
				}
			}
		}

		const size_t arg_offset = table_op ? 1 : 0;

		for (size_t i=arg_offset; i<e->args.size(); i++) {
			auto a = get(e->args[i]);
			ASSERT(a);
			auto reference = factory.reference(a->var);

			const auto& type = e->args[i]->props.type.arity[0].type;

			if (a->scalar) {
				reference = factory.function("ADDRESS_OF", reference);
			} else {
				reference = factory.function("IFujiVector::USE_GET_FIRST", reference);
			}

			reference = factory.cast(type+"*", reference);

			arg_exprs.emplace_back(reference);
			arg_types.emplace_back(type);
		}

		const bool patch_func = table_op && column && table_out;

		dest_var = new_dest(res_type0, !patch_func);

		if (patch_func) {
			ASSERT(col.size() > 0);
			ASSERT(dest_var);
			statements.emplace_back(factory.effect(
				factory.function("IFujiVector::SET_FIRST",
					factory.reference(dest_var),
					factory.literal_from_str("thread." + tbl + "->col_" + col + ".get() /* table out */"))
			));
		}

		// prefix arguments with sel, num
		{
			clite::ExprList tmp;

			clite::ExprPtr sel;
			clite::ExprPtr num;

			if (e->pred) {
				auto pexpr = get(e->pred);

				if (pexpr->scalar) {
					sel = factory.literal_from_str("nullptr");
					num = factory.reference(pexpr->var);
					ASSERT(pexpr->var->type == "sel_t");
				} else {
					sel = factory.function("IFujiVector::USE_GET_FIRST",
						factory.reference(pexpr->var));
					assert_scalar_num(pexpr->num);
					num = factory.reference(pexpr->num->var);
				}

			} else {
				sel = factory.literal_from_str("nullptr");

				for (auto& a : e->args) {
					if (num) {
						continue;
					}

					auto arg = get(a);
					if (!arg || !arg->num) {
						continue;
					}

					num = factory.reference(arg->num->var);
				}

				if (!num) {
					num = factory.reference(const_vector_size());
				}
			}

			if (!e->fun.compare("check")) {
#ifdef TRACE
				std::string dbg_check_name("key_checks" + std::to_string(m_codegen.key_checks.size()));
				m_codegen.key_checks.push_back(dbg_check_name);
				statements.emplace_back(factory.plain("g_check_line = __LINE__;"));
				statements.emplace_back(factory.effect(factory.function(dbg_check_name + " += ", num)));
#endif
			}

			sel = factory.cast("sel_t*", sel);

			tmp.emplace_back(sel);
			tmp.emplace_back(num);
			tmp.emplace_back(factory.cast(res_type0 + "*", factory.function("IFujiVector::SELF_GET_FIRST",
				factory.reference(dest_var))));

			for (auto&& expr : arg_exprs) {
				tmp.emplace_back(expr);
			}

			arg_exprs = std::move(tmp);
		}

		if (e->is_select()) {
			auto num_var = get_fragment().new_var(unique_id(),
				"sel_t", clite::Variable::Scope::Local);
			// num_var->prevent_promotion = true;
			dest_num = num_var;

			clite::ExprPtr res_expr;

			if (!e->fun.compare("selunion")) {
				auto left = get(e->args[0]);
				auto right = get(e->args[1]);

				ASSERT(left->num);
				ASSERT(right->num);

				clite::ExprList union_args = {
					factory.function("(sel_t*)IFujiVector::SELF_GET_FIRST", factory.reference(dest_var)),
					factory.function("(sel_t*)IFujiVector::USE_GET_FIRST", factory.reference(left->var)),
					factory.reference(left->num->var),
					factory.function("(sel_t*)IFujiVector::USE_GET_FIRST",factory.reference(right->var)),
					factory.reference(right->num->var)
				};

				res_expr = factory.function("ComplexFuncs::selunion", union_args);
			} else {
				res_expr = factory.function(
					primitive_name(PrimitiveSignature {e->fun, res_type0, arg_types}),
					arg_exprs);
			}

			is_predicate = true;

			m_codegen.register_reset_var_vec(dest_var);
			// m_codegen.register_reset_var_int(dest_num);

			statements.emplace_back(factory.assign(dest_num, res_expr));
		} else  {
			if (e->pred) {
				auto pred = get(e->pred);
				ASSERT(pred);
				ASSERT(pred->var);

				if (pred->scalar) {
					dest_num = pred->var;
					ASSERT(dest_num->type == "sel_t");
				} else {
					dest_num = pred->num->var;
				}
			} else {
#if 1
				for (auto& a : e->args) {
					if (dest_num) {
						continue;
					}

					auto arg = get(a);
					if (!arg || !arg->num) {
						continue;
					}

					dest_num = arg->num->var;
				}
#endif
				if (!dest_num) {
					dest_num = const_vector_size();
				}
			}

			statements.emplace_back(factory.effect(
				factory.function(
					primitive_name(PrimitiveSignature {e->fun, res_type0, arg_types}),
					arg_exprs)));
		}

		match = true;
	}

	if (!match && e->type == Expression::Constant) {
		ASSERT(e->props.type.arity.size() == 1 && "Must be scalar");

		dest_var = new_dest(res_type0, true);
		dest_num = const_vector_size();

		std::string value(e->fun);

		if (res_type0 == "varchar") {
			value = "\"" + value + "\"";
		}

		dest_var->ctor_body.emplace_back(factory.effect(
				factory.function("IFujiVector::BROADCAST<" + res_type0 + ">",
				factory.reference(dest_var),
				factory.literal_from_str(value),
				factory.reference(const_vector_size())
			)));

		match = true;
	}

	ASSERT(dest_var);
	ASSERT(dest_num);

	auto result = std::make_shared<VectorExpr>(*this, dest_var,
		is_predicate, "expr " + e->fun, false, false, res_type0
		// e->props.type.arity[0].type
		);
	result->num = std::make_shared<VectorExpr>(*this, dest_num,
		false, "num " + e->fun, true, true, "sel_t");
	assert_scalar_num(result->num);

	put(e, result);

	clite::Builder builder(*m_codegen.get_current_state());
	builder << builder.comment("expr: " + e->fun);
	builder << statements;

	ASSERT(match);
}

void
VectorDataGen::gen_output(StmtPtr& e)
{
	ASSERT(e->type == Statement::Emit);

	auto pred = e->pred ? get(e->pred) : nullptr;

	auto& child = e->expr;
	ASSERT(child && !child->fun.compare("tappend"));

	std::ostringstream impl;

	clite::Builder builder(m_codegen.get_current_state());
	clite::StmtList statements;
	statements.emplace_back(builder.plain("{"));
	statements.emplace_back(builder.plain("auto output = [&] (sel_t* sel, sel_t num) {"));
	statements.emplace_back(builder.plain("for (sel_t i=0; i<num; i++) {"));
	statements.emplace_back(builder.plain("query.result.begin_line();"));

	int child_idx = 0;
	for (auto& arg : child->args) {
		auto c = get(arg);

		const auto tpe = arg->props.type.arity[0].type;
		statements.emplace_back(builder.effect(
			builder.function("IFujiVector::OUTPUT<" + tpe + ">",
				builder.literal_from_str("query"),
				builder.reference(c->var),
				builder.literal_from_str("sel ? sel[i] : i"))));

		child_idx++;
	}

	statements.emplace_back(builder.plain("query.result.end_line();"));
	statements.emplace_back(builder.plain("}"));
	statements.emplace_back(builder.plain("};"));

	clite::ExprPtr num_expr;
	clite::ExprPtr sel_expr;

	if (pred) {
		num_expr = builder.reference(pred->num->var);
		sel_expr = builder.function("IFujiVector::USE_GET_FIRST",
			builder.reference(pred->var));
	} else {
		for (auto& _arg : child->args) {
			auto arg = get(_arg);
			if (num_expr || !arg->num) {
				continue;
			}
			num_expr = builder.reference(arg->num->var);
		}

		if (!num_expr) {
			num_expr = builder.reference(const_vector_size());
		}
		ASSERT(num_expr);
		sel_expr = builder.literal_from_str("nullptr");
	}

	sel_expr = builder.cast("sel_t*", sel_expr);

	statements.emplace_back(builder.effect(builder.function("output", sel_expr, num_expr)));	
	statements.emplace_back(builder.plain("}"));

	builder << statements;
}

DataGenExprPtr
VectorDataGen::gen_emit(StmtPtr& e, Lolepop* op)
{
	const std::string op_name(op ? op->name : "null_op");

	clite::Builder builder(*m_codegen.get_current_state());
	builder << builder.comment("emit " + op_name);

	LOG_DEBUG("EMIT from %s\n", op_name.c_str());

	DataGenExprPtr new_pred;
	if (e->expr->type == Expression::Type::Function &&
			!e->expr->fun.compare("tappend")) {

		VectorExpr* mask = nullptr;

		for (auto& arg : e->expr->args) {
			VectorExpr* expr = get(arg);

			ASSERT(expr);

			if (!expr->num.get()) {
				continue;
			}

			if (mask) {
				// ASSERT(mask == expr->mask.get());
			} else {
				mask = (VectorExpr*)expr->num.get();
			}
		}

		auto pred = get_ptr(e->expr->pred);


		// TODO: buffer tuples

		// create new mask

		if (pred) {
			printf("PRED\n");

			auto p = std::dynamic_pointer_cast<VectorDataGen::VectorExpr>(pred);

			ASSERT(p->num);
			return p;
		} else {
			if (mask) {
				std::string postfix;
				if (mask->num) {
					postfix = "/w/num";

					new_pred = std::make_shared<VectorExpr>(*this, mask->var, true,
						"emit:" + op_name + "/w/mask" + postfix, true, true, kPredType);
				} else {
					postfix = "/nonum";
					auto null_var = get_fragment().new_var(unique_id(), "IFujiVector", kVarDestScope);

					m_codegen.register_reset_var_vec(null_var);

					null_var->prevent_promotion = true;

					auto new_pred = std::make_shared<VectorExpr>(*this, null_var, true,
						"emit:" + op_name + "/w/mask" + postfix, false, false, kPredType + "*");

					auto new_num = std::make_shared<VectorExpr>(*this, mask->var, true,
						"emit:" + op_name + "/w/mask" + postfix, true, true, kPredType);
					new_pred->num = new_num;

					return new_pred;
				}
			} else {
				printf("FULL\n");
				new_pred = new_non_selective_predicate();
			}
		}
	} else {
		ASSERT(false && "todo");
	}

	return new_pred;
}

void
VectorDataGen::gen_extra(clite::StmtList& stmts, ExprPtr& e) 
{
	(void)stmts;
	(void)e;

	auto expr = get(e);

	if (e->is_get_pos()) {
		auto num = get_fragment().new_var(unique_id(), "sel_t",
			clite::Variable::Scope::Local);
		// num->prevent_promotion = true;

		clite::Builder builder(*m_codegen.get_current_state());
		builder << builder.assign(num,
			builder.function("POSITION_NUM", builder.reference(expr->var)));

		ASSERT(!expr->num);
		expr->num = std::make_shared<VectorExpr>(*this,
			num, false, "extra " + e->fun, true, true, kPredType);
		assert_scalar_num(expr->num);
		return;
	}

	const auto& n = e->fun;
	clite::Factory factory;

	bool is_global_aggr = false;
	e->is_aggr(&is_global_aggr);
	if (is_global_aggr) {
		clite::StmtList statements;

		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		LOG_ERROR("global %s\n", n.c_str());


		const std::string tpe(e->props.type.arity.size() > 0 ?
			e->props.type.arity[0].type : std::string(""));

		std::string arg_type("i64");

		std::string agg_type;
		std::string comb_type;
		if (n == "aggr_gcount") {
			agg_type = "aggr_direct_gcount";
			comb_type = "SUM";
		} else if (n == "aggr_gsum") {
			agg_type = "aggr_direct_gsum";
			comb_type = "SUM";
		} else {
			ASSERT(false);
		}

		auto acc = new_global_aggregate(tpe, tbl, col, comb_type);

		clite::ExprPtr arg;

		if (n == "aggr_gcount") {
			arg = factory.literal_from_str("nullptr");
		} else {
			arg = factory.function("IFujiVector::USE_GET_FIRST",
				factory.reference(expr2get0(e->args[1])));

			arg_type = e->args[1]->props.type.arity[0].type;
		}

		auto pred = e->pred ? get(e->pred) : nullptr;
		clite::ExprPtr num_expr;
		clite::ExprPtr sel_expr;

		if (pred) {
			num_expr = factory.reference(pred->num->var);
			sel_expr = factory.function("IFujiVector::USE_GET_FIRST",
				factory.reference(pred->var));
		} else {
			for (auto& _arg : e->args) {
				auto arg = get(_arg);
				if (num_expr || !arg->num) {
					continue;
				}
				num_expr = factory.reference(arg->num->var);
			}

			if (!num_expr) {
				num_expr = factory.reference(const_vector_size());
			}
			ASSERT(num_expr);
			sel_expr = factory.literal_from_str("nullptr");
		}

		sel_expr = factory.cast("sel_t*", sel_expr);
		arg = factory.cast(arg_type + "*", arg);

		stmts.emplace_back(factory.effect(
			factory.function("vec_" + agg_type + "__" + tpe + "__" + arg_type + "_col", {
				sel_expr, num_expr,
				factory.function("ADDRESS_OF", factory.reference(acc)),
				arg,
			})
		));

		// add predicate and num

		return;
	}
}

DataGenBufferPosPtr
VectorDataGen::write_buffer_get_pos(const DataGenBufferPtr& buffer,
	clite::Block* flush_state, const clite::ExprPtr& num,
	const DataGenExprPtr& pred, const std::string& dbg_flush)
{
	(void)num;
	clite::Factory f;

	clite::Builder builder(*m_codegen.get_current_state());

	// does this make a difference?
	// propagate to #78
	auto p = std::dynamic_pointer_cast<VectorDataGen::VectorExpr>(pred);
	ASSERT(p);

	clite::ExprPtr n;
	clite::ExprPtr n2;

	if (p->num) {
		n = f.literal_from_int(get_unroll_factor());
		n2 = f.reference(p->num->var);
	} else {
		n = f.reference(p->var);
		n2 = n;
	}

	builder << builder.log_trace("%s: write_buffer_get_pos num=%d", {
		f.literal_from_str("g_pipeline_name"),
		n2
	});

	return DataGen::write_buffer_get_pos(buffer, flush_state, n, pred,
		dbg_flush);
}

DataGenBufferPosPtr
VectorDataGen::read_buffer_get_pos(const DataGenBufferPtr& buffer,
	const clite::ExprPtr& __num, clite::Block* empty, const std::string& dbg_refill)
{
	(void)__num;

	clite::Factory factory;
	auto r = DataGen::read_buffer_get_pos(buffer,
		factory.literal_from_int(get_unroll_factor()), empty, dbg_refill);

	auto num = get_fragment().new_var(unique_id(), "sel_t",
		clite::Variable::Scope::Local);

	// num->prevent_promotion = true;

	clite::Builder builder(*m_codegen.get_current_state());
	builder << builder.assign(num,
		builder.function("BUFFER_CONTEXT_LENGTH", builder.reference(r->var)));

	// ASSERT(!r->num);
	r->num = builder.reference(num);
	r->pos_num_mask = std::make_shared<VectorExpr>(*this, num,
		true, "read_buffer_pos_num", true, true, kPredType);

	return r;
}

DataGenBufferColPtr
VectorDataGen::write_buffer_write_col(const DataGenBufferPosPtr& pos,
	const DataGenExprPtr& _e, const std::string& dbg_name)
{
	LOG_TRACE("write_buffer state=%p\n", m_codegen.get_current_state());

	auto e = std::dynamic_pointer_cast<VectorExpr>(_e);
	ASSERT(e);


	std::string var_type(e->scal_type);

	clite::Builder builder(*m_codegen.get_current_state());

	clite::VarPtr input_var(_e->var);

	bool is_pred = false;
	
	builder << builder.comment("is_predicate=" + std::to_string(e->predicate));
	builder << builder.comment("scal_type = " + var_type);
	builder << builder.comment("type = " + e->var->type);
	
	// e->scal_type == "pred_t"
	if (e->predicate && pos->buffer->in_config != pos->buffer->out_config) {
		is_pred = true;

		builder << builder.comment("translating ...");
	}


	if (is_pred) {
		var_type = "pred_t";
	}

	LOG_DEBUG("VectorDataGen::write_buffer_write_col: dbg_name=%s, type=%s\n",
		dbg_name.c_str(), var_type.c_str());
	auto buffer_var = get_fragment().new_var(unique_id(), var_type + "*",
			clite::Variable::Scope::ThreadWide);

	auto column = std::make_shared<DataGenBufferCol>(pos->buffer, buffer_var,
		var_type, e->predicate);

	buffer_ensure_col_exists(column, var_type);

	pos->buffer->columns.push_back(column);

	auto pos_var = builder.reference(pos->var);
	auto col_var = builder.reference(column->var);
	auto length = builder.function("BUFFER_CONTEXT_LENGTH", pos_var);
	auto offset = builder.function("BUFFER_CONTEXT_OFFSET", pos_var);

	builder << builder.comment("write buffer '" + dbg_name + "'");

	clite::ExprPtr data = builder.function("IFujiVector::USE_GET_FIRST", builder.reference(e->var));
	if (is_pred && !e->num) {
		data = builder.literal_from_str("nullptr");
	}

	std::string func;
	clite::ExprList func_args {
		col_var,
		data,
		offset,
		builder.literal_from_str("nullptr"), // FIXME
		length
	};

	if (is_pred) {
		auto n = e->num;
		ASSERT(n);
		func_args.push_back(builder.reference(n->var));
#if 0
		if (e->num) {
		} else {
			func_args.push_back(builder.reference(e->var));
		}
#endif
	}

	if (is_pred) {
		func = "Vectorized::predicate_buf_write";
		func_args.push_back(builder.function("STRINGIFY",
			builder.literal_from_str(column->var->name)));
		func_args.push_back(builder.literal_from_str("__LINE__"));
	} else {
		func = "Vectorized::buf_write<" + e->scal_type + ">";
	}

	auto function = builder.function(func, func_args);


	builder << builder.effect(function);

	return column;
}

DataGenExprPtr
VectorDataGen::read_buffer_read_col(const DataGenBufferPosPtr& pos,
	const DataGenBufferColPtr& column, const std::string& dbg_name)
{
	
	std::string tpe(column->scal_type);

	if (tpe.empty()) {
		// non SIMDizable
		ASSERT(false && "Todo: non simd");
	}


	bool is_pred = false;
	if (column->predicate && pos->buffer->in_config != pos->buffer->out_config) {
		is_pred = true;
	}

	const std::string vector_type(is_pred ? std::string("sel_t") : tpe);
	const std::string prefix("FujiAllocatedVector<" + vector_type + "," + std::to_string(m_unroll_factor) + ">");

	auto dest_var = get_fragment().new_var(unique_id(), prefix,
		clite::Variable::Scope::Local);
	dest_var->prevent_promotion = true;

	clite::Builder builder(*m_codegen.get_current_state());

	builder
		<< builder.comment("read_buffer '" + column->var->name + "'" +
			" into '" + dbg_name + "'");

	ASSERT(pos->pos_num_mask);
	ASSERT(pos->pos_num_mask->var);

	auto mask = std::dynamic_pointer_cast<VectorExpr>(pos->pos_num_mask);
	ASSERT(mask);

	auto pos_var = builder.reference(pos->var);
	auto col_var = builder.reference(column->var);
	auto length = builder.reference(mask->var); // builder.function("BUFFER_CONTEXT_LENGTH", pos_var);
	auto offset = builder.function("BUFFER_CONTEXT_OFFSET", pos_var);

	LOG_DEBUG("VectorDataGen::read_buffer_read_col: dbg_name=%s, type=%s, scal_type=%s, predicate=%d\n",
		dbg_name.c_str(), tpe.c_str(), tpe.c_str(), column->predicate);

#if 0
	if (column->vec_num_only) {
		std::string func("Vectorized::predicate_buf_read");

		builder << builder.comment("vec_num_only");
		auto function = builder.function(func, clite::ExprList {
			builder.literal_from_str("nullptr"),
			col_var,
			offset, builder.literal_from_str("nullptr"),
			length
		});

		clite::VarPtr num;
		num = get_fragment().new_var(unique_id(), "sel_t",
			clite::Variable::Scope::Local);
		num->prevent_promotion = true;
		builder << builder.assign(num, function);

		auto ptr = std::make_shared<VectorExpr>(*this, num,
			column->predicate, dbg_name, true, true, column->scal_type);

		return ptr;
	}
#endif

	std::string func;
	clite::ExprList func_args {
		builder.function("IFujiVector::SELF_GET_FIRST", builder.reference(dest_var)),
		col_var,
		offset, builder.literal_from_str("nullptr"),
		length
	};

	if (is_pred) {
		func = "Vectorized::predicate_buf_read";
		func_args.push_back(builder.function("STRINGIFY",
			builder.literal_from_str(column->var->name)));
		func_args.push_back(builder.literal_from_str("__LINE__"));
	} else {
		func = "Vectorized::buf_read<" + tpe + ">";
	}


	auto function = builder.function(func, func_args);

	clite::VarPtr num;
	if (is_pred) {
		num = get_fragment().new_var(unique_id(), "sel_t",
			clite::Variable::Scope::Local);
		num->prevent_promotion = true;
		builder << builder.assign(num, function);

		m_codegen.register_reset_var_vec(dest_var);
	} else {
		builder << builder.effect(function);
	}

	auto ptr = std::make_shared<VectorExpr>(*this, dest_var,
		column->predicate, dbg_name, false, false, column->scal_type);

	if (is_pred) {
		ASSERT(num);
		ptr->num = std::make_shared<VectorExpr>(*this, num,
			column->predicate, dbg_name, true, true, "sel_t");
	} else {
		ptr->num = pos->pos_num_mask;
	}
	return ptr;
}

void
VectorDataGen::prefetch(const ExprPtr& e, int temporality)
{
	bool match = false;
	const auto& n = e->fun;

	std::ostringstream impl;

	bool table_in = false;
	bool table_out = false;

	ASSERT(e->is_table_op(&table_out, &table_in));
	std::string tbl, col;
	e->get_table_column_ref(tbl, col);

	ASSERT(e->pred);

	clite::Builder builder(*m_codegen.get_current_state());

	clite::ExprPtr num;
	clite::ExprPtr sel;


	auto pexpr = get(e->pred);
	ASSERT(pexpr);

	if (pexpr->scalar) {
		sel = builder.literal_from_str("nullptr");
		num = builder.reference(pexpr->var);
		ASSERT(pexpr->var->type == "sel_t");
	} else {
		sel = builder.function("IFujiVector::USE_GET_FIRST",
			builder.reference(pexpr->var));
		assert_scalar_num(pexpr->num);
		num = builder.reference(pexpr->num->var);
	}

	sel = builder.cast("sel_t*", sel);

#if 0
	if (pred->num) {
		num_expr = builder.reference(pred->num->var);
		sel_expr = builder.cast("sel_t*", builder.function("IFujiVector::GET_FIRST",
			builder.reference(pred->var)));
	} else {
		num_expr = builder.reference(pred->var);
		sel_expr = builder.cast("sel_t*", builder.literal_from_str("nullptr"));
	}
#endif

	if (!match && !n.compare("bucket_lookup")) {
		auto index = expr2get0(e->args[1]);

		auto hash_mask = builder.literal_from_str(get_hash_mask_code(tbl));
		auto hash_index = builder.literal_from_str(get_hash_index_code(tbl));

		builder << builder.effect(builder.function("prefetch_bucket_lookup", {
				sel, num,
				builder.literal_from_int(temporality),
				hash_index,
				builder.cast("u64*",
					builder.function("IFujiVector::USE_GET_FIRST", builder.reference(index))),
				hash_mask
				}));

		match = true;
	}

	if (!match && !n.compare("bucket_next")) {
		auto index = get_bucket_index_expr(e);

		builder << builder.effect(builder.function("prefetch_bucket_next", {
			sel, num,
			builder.literal_from_int(temporality),
			builder.cast("void**", builder.function("IFujiVector::USE_GET_FIRST", index)),
			builder.literal_from_str("(u64)thread." + tbl + "->next_offset")
		}));
		match = true;
	}

	if (!match) {
		auto index = get_bucket_index_expr(e);

		auto col_name = replace_all(e->props.first_touch_random_access,
			"col_", "");

		builder << builder.effect(builder.function("prefetch_bucket", {
			sel, num,
			builder.literal_from_int(temporality),
			builder.cast("void**", builder.function("IFujiVector::USE_GET_FIRST", index)),
			builder.literal_from_str("(u64)thread." + tbl + "->offset_" + col_name)
		}));
		match = true;
	}

	ASSERT(match);
}

std::string
VectorDataGen::get_flavor_name() const
{
	return m_name;
}

clite::VarPtr
VectorDataGen::const_vector_size()
{
	if (!var_const_vector_size) {
		var_const_vector_size = get_fragment().new_var(unique_id(),
			"sel_t", clite::Variable::Scope::ThreadWide, true,
			std::to_string(m_unroll_factor));
		var_const_vector_size->prevent_promotion = true;

		ASSERT(var_const_vector_size);
	}
	
	return var_const_vector_size;
}


clite::ExprPtr
VectorDataGen::is_predicate_non_zero(const DataGenExprPtr& _pred)
{
	clite::Factory f;

	ASSERT(_pred);
	auto pred = std::dynamic_pointer_cast<VectorExpr>(_pred);

	ASSERT(pred);

	if (pred->scalar) {
		return DataGen::is_predicate_non_zero(pred);
	}
	ASSERT(pred->num);

	return f.function("!!", f.reference(pred->num->var));
}
