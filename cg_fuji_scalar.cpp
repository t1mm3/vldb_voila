#include "cg_fuji_scalar.hpp"
#include "cg_fuji.hpp"
#include "runtime.hpp"
#include "cg_fuji_control.hpp"
#include "utils.hpp"

static const std::string kPredType = "pred_t";
static const std::string kPredTrue = "true";
static const std::string kPredFalse = "false";

static const std::string kColPrefix = "col_";

#define EOL std::endl


ScalarDataGen::ScalarExpr*
ScalarDataGen::get(const ExprPtr& e)
{
	LOG_TRACE("ScalarDataGen::get(%p)\n", e.get());
	ASSERT(e);
	auto p = get_ptr(e);
	if (!p) {
		LOG_ERROR("ScalarDataGen::get(%p): Pointer is NULL\n",
			e.get());
		// ASSERT(p);
		return nullptr;
	}
	auto r = dynamic_cast<ScalarExpr*>(p.get());
	if (!r) {
		LOG_ERROR("Not castable to ScalarDataGen::ScalarExpr: this='%s', p='%s'\n",
			get_flavor_name().c_str(), p->src_data_gen.c_str());
		ASSERT(r && "Must be castable to DataGen's type");
	}
	return r;
}


clite::VarPtr
ScalarDataGen::expr2get0(ExprPtr& arg)
{
	auto a = get(arg);
	ASSERT(a);
	return a->var;
}


DataGenExprPtr
ScalarDataGen::new_simple_expr(const DataGen::SimpleExprArgs& args)
{
	const std::string vid(args.id.empty() ? unique_id() : args.id);
	auto var = get_fragment().new_var(vid, args.type, args.scope);
	return std::make_shared<ScalarExpr>(*this, var, args.predicate);
}

DataGenExprPtr
ScalarDataGen::new_non_selective_predicate()
{
	auto& state = *m_codegen.get_current_state();
	clite::Builder builder(state);
	auto fragment = get_fragment();

	auto dest_var = fragment.new_var(unique_id(), kPredType,
		clite::Variable::Scope::ThreadWide);

	builder << builder.assign(dest_var, builder.literal_from_str(kPredTrue));

	return std::make_shared<ScalarExpr>(*this, dest_var,
		true);
}

DataGenExprPtr
ScalarDataGen::clone_expr(const DataGenExprPtr& a)
{
	ASSERT(a);

	return std::make_shared<ScalarExpr>(*this,
		get_fragment().clone_var(unique_id(), a->var),
		a->predicate);
}

void
ScalarDataGen::copy_expr(DataGenExprPtr& res, const DataGenExprPtr& a)
{
	ASSERT(res);
	ASSERT(a);

	auto& state = *m_codegen.get_current_state();
	clite::Builder builder(state);
	
	builder << builder.assign(res->var, builder.reference(a->var));

	ASSERT(res.get() != a.get());
	ASSERT(res->var.get() != a->var.get());
}

void
ScalarDataGen::gen_expr(ExprPtr& e)
{
	gen(e, false);
}

void
ScalarDataGen::gen_pred(ExprPtr& e)
{
	gen(e, true);
}

void
ScalarDataGen::gen(ExprPtr& e, bool pred)
{
	(void)pred;
	std::ostringstream impl;
	std::ostringstream init;
	std::string id = unique_id();

	bool match = false;
	bool create_local = true;
	bool has_result = true;
	bool new_variable = true;

	const auto& n = e->fun;
	const auto arity = e->props.type.arity.size();
	const std::string res_type0(arity > 0 ?
		e->props.type.arity[0].type : std::string(""));

	std::string tpe = res_type0;

	auto dest_var = get_fragment().new_var(id, tpe,
		clite::Variable::Scope::Local);
	bool needs_predication = true;

	clite::StmtList statements;
	clite::Factory factory;

	if (!match && e->type == Expression::Constant) {
		auto constant = e->fun;
		const auto& type = e->props.type.arity[0].type;
		const bool is_string = !type.compare("varchar");

		dest_var->constant = true;

		if (is_string) {
			dest_var->default_value = factory.str_literal(constant);
		} else {
			dest_var->default_value = factory.literal_from_str(constant);
		}
		match = true;
	}

	ASSERT(match || e->type == Expression::Function);

	auto infix_op = [&] (const std::string& voila, const std::string& c) {
		if (match || n.compare(voila)) {
			return;
		}

		auto a = get(e->args[0]);
		auto b = get(e->args[1]);

		ASSERT(a && b);

		statements.emplace_back(factory.assign(dest_var,
			factory.function(c, factory.reference(a->var), factory.reference(b->var))));

		match = true;
	};

	infix_op("eq", "==");
	infix_op("ne", "!=");
	infix_op("lt", "<");
	infix_op("le", "<=");
	infix_op("gt", ">");
	infix_op("ge", ">=");

	infix_op("and", "&&");
	infix_op("or", "||");

	infix_op("selunion", "||");

	infix_op("add", "+");
	infix_op("sub", "-");
	infix_op("mul", "*");

	if (!match && e->is_select()) {
		ASSERT(!n.compare("seltrue") || !n.compare("selfalse"));

		ASSERT(e->args[0]);
		ASSERT(e->args.size() == 1);
		auto a = get(e->args[0]);
		ASSERT(a);
		clite::ExprPtr val = factory.reference(a->var);

		clite::ExprPtr pred_cid;
		std::string fname;

		if (!match && !n.compare("seltrue")) {
			fname = "!!";
			match = true;
		}

		if (!match && !n.compare("selfalse")) {
			fname = "!";
			match = true;
		}

		if (match) {
			val = factory.function(fname, val);

			if (e->pred) {
				val = factory.function("&&",
					val, factory.reference(get(e->pred)->var));
			}

			needs_predication = false;
			statements.emplace_back(factory.assign(dest_var, val));
		}
	}

	if (!match && !n.compare("hash")) {
		statements.emplace_back(factory.assign(dest_var,
			factory.function(
				"voila_hash<" + e->args[0]->props.type.arity[0].type+ ">::hash",
				factory.reference(get(e->args[0])->var))));
		match = true;
	}

	if (!match && !n.compare("rehash")) {
		statements.emplace_back(factory.assign(dest_var,
			factory.function(
				"voila_hash<" + e->args[1]->props.type.arity[0].type+ ">::rehash",
				factory.reference(get(e->args[0])->var),
				factory.reference(get(e->args[1])->var))));
		match = true;
	}

	// more or less refactored copy&paste from cg_hyper.cpp

	if (!match && !n.compare("scan")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		auto index = expr2get0(e->args[1]);

#if 0
		statements.emplace_back(factory.assign(dest_var,
			factory.function("SCALAR_SCAN",
				factory.literal_from_str(tbl), factory.literal_from_str(col),
				factory.function("POSITION_OFFSET", factory.reference(index)))));
#else
		auto scan_ptr = get_fragment().new_var(unique_id(), tpe + "*",
			clite::Variable::Scope::ThreadWide, false,
			"(" + tpe + "*)(thread." + tbl + "->col_" + col + ".get(0))");


		statements.emplace_back(factory.assign(dest_var,
			factory.array_access(factory.reference(scan_ptr),
				factory.function("POSITION_OFFSET", factory.reference(index)))
		));
#endif

		match = true;
	}

	bool table_in = false;
	bool table_out = false;

	if (!match && e->is_table_op(&table_out, &table_in)) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		if (!match && e->is_aggr()) {
			auto arg = expr2get0(e->args[2]);
			auto fetched = access_column(tbl, kColPrefix + col, get_bucket_index_expr(e));

			std::string type;
			if (n == "aggr_count") {
				type = "COUNT";
			} else if (n == "aggr_sum") {
				type = "SUM";
			} else if (n == "aggr_min") {
				type = "MIN";
			} else if (n == "aggr_max") {
				type = "MAX";
			} else {
				ASSERT(false);
			}
			statements.emplace_back(factory.effect(
					factory.function("SCALAR_AGGREGATE",
						factory.literal_from_str(type), fetched, factory.reference(arg)
					)
				));
			new_variable = false;
			match = true;
		}

		if (!match) {
			if (!match && !n.compare("gather")) {
				statements.emplace_back(factory.assign(dest_var,
					access_column(tbl, kColPrefix + col, get_bucket_index_expr(e))));
				match = true;
			}

			if (!match && !n.compare("check")) {
				auto key = factory.reference(expr2get0(e->args[2]));
				auto fetched = access_column(tbl, kColPrefix + col, get_bucket_index_expr(e));
				statements.emplace_back(factory.assign(dest_var, factory.function("==", fetched, key)));

#ifdef TRACE
				statements.emplace_back(factory.log_error("%s: %s: check data=%d key=%d (%s, %s)", {
					factory.literal_from_str("g_pipeline_name"),
					factory.literal_from_str("g_buffer_name"),
					fetched, key,
					factory.function("STRINGIFY", fetched),
					factory.function("STRINGIFY", key)
				}));

				std::string dbg_check_name("key_checks" + std::to_string(m_codegen.key_checks.size()));
				m_codegen.key_checks.push_back(dbg_check_name);
				statements.emplace_back(factory.plain(dbg_check_name + "++;"));
#endif

				match = true;
			}

			if (!match && !n.compare("scatter")) {
				statements.emplace_back(factory.assign(access_column(tbl, kColPrefix + col, get_bucket_index_expr(e)),
					factory.reference(expr2get0(e->args[2]))));

				match = true;
				new_variable = false;
			}

			if (!match && !n.compare("read")) {
				statements.emplace_back(
					factory.assign(dest_var,
						factory.function("ACCESS_BUFFERED_ROW",
							factory.reference(expr2get0(e->args[1])),
							factory.literal_from_str(get_row_type(tbl)),
							factory.literal_from_str(col)
						)
					)
				);

				match = true;
			}

			if (!match && !n.compare("write")) {
				statements.emplace_back(
					factory.assign(
						factory.function("ACCESS_BUFFERED_ROW",
							factory.reference(expr2get0(e->args[1])),
							factory.literal_from_str(get_row_type(tbl)),
							factory.literal_from_str(col)
						),
						factory.reference(expr2get0(e->args[2]))
					)
				);

				match = true;
				new_variable = false;
			}

			if (!match && !n.compare("bucket_next")) {
				auto index = get_bucket_index_expr(e);

				// statements.emplace_back(factory.log_debug("bucket_next", {}));
				statements.emplace_back(factory.assign(dest_var,
					access_column(tbl, "next", get_bucket_index_expr(e))));

				match = true;
			}

			if (!match && !n.compare("bucket_lookup")) {
				auto index = factory.reference(expr2get0(e->args[1]));

				auto hash_index = get_hash_index_var(tbl);
				auto hash_mask = get_hash_mask_var(tbl);

				// statements.emplace_back(factory.log_debug("bucket_lookup", {}));

				statements.emplace_back(
					factory.assign(dest_var,
						factory.function("SCALAR_BUCKET_LOOKUP", {
							access_table(tbl),
							index,
							factory.reference(hash_index),
							factory.reference(hash_mask)
						})));

				match = true;
			}

			if (!match && !n.compare("bucket_insert")) {
				auto index = factory.reference(expr2get0(e->args[1]));

				auto hash_index = get_hash_index_var(tbl);
				auto hash_mask = get_hash_mask_var(tbl);

				statements.emplace_back(
					factory.assign(dest_var,
						factory.function("SCALAR_BUCKET_INSERT",
							access_table(tbl),
							index,
							factory.literal_from_str(get_row_type(tbl))
						)));

				statements.emplace_back(
					factory.assign(factory.reference(hash_index),
						factory.literal_from_str(get_hash_index_code(tbl))));
				statements.emplace_back(
					factory.assign(factory.reference(hash_mask),
						factory.literal_from_str(get_hash_mask_code(tbl))));

				match = true;
			}
		}

		ASSERT(match);
	}

	if (!match) {
		auto cast2 = split(n, 'T');

		if (cast2.size() > 1 && !cast2[0].compare("cast")) {
			statements.emplace_back(factory.assign(dest_var,
			factory.cast(
				cast2[1],
				factory.reference(get(e->args[0])->var))));

			match = true;
		}
	}

	// assume some function
	if (!match) {
		clite::ExprList args;
		for (auto& a : e->args) {
			args.emplace_back(factory.reference(get(a)->var));
		}

		statements.emplace_back(factory.assign(dest_var,
			factory.function( e->fun,args)));

		match = true;
	}

	if (!statements.empty()) {
		clite::Builder builder(m_codegen.get_current_state());

		if (needs_predication && e->pred) {
			auto pred = get(e->pred);
			ASSERT(pred);
			builder << builder.predicated(
				builder.reference(pred->var), statements);
		} else {
			builder << statements;
		}
	}

	if (match && create_local) {
		if (!has_result) {
			ASSERT(!new_variable);
		}

		put(e, std::make_shared<ScalarExpr>(*this, dest_var,
			res_type0 == "pred_t"));

		printf("get(%p): %p\n", e.get(), get(e));

		return;
	}

	ASSERT(match);

	return;
}

void
ScalarDataGen::gen_output(StmtPtr& e)
{
	ASSERT(e->type == Statement::Emit);

	auto pred = e->pred ? get(e->pred) : nullptr;

	auto& child = e->expr;
	ASSERT(child && !child->fun.compare("tappend"));

	std::ostringstream impl;

	clite::Builder builder(m_codegen.get_current_state());
	clite::StmtList statements;
	
	statements.emplace_back(builder.plain("query.result.begin_line();"));

	for (auto& arg : child->args) {
		auto c = get(arg);

		statements.emplace_back(builder.effect(
			builder.function("SCALAR_OUTPUT",
				builder.reference(c->var))));
	}

	statements.emplace_back(builder.plain("query.result.end_line();"));

	if (pred) {
		builder << builder.predicated(builder.reference(pred->var),
			statements);
	} else {
		builder << statements;
	}
}

DataGenExprPtr
ScalarDataGen::gen_emit(StmtPtr& e, Lolepop* op)
{
	(void)op;

	DataGenExprPtr new_pred;
	if (e->expr->type == Expression::Type::Function &&
			!e->expr->fun.compare("tappend")) {

		new_pred = get_ptr(e->expr->pred);
		if (!new_pred) {
			return new_non_selective_predicate();
		}
	} else {
		ASSERT(false && "todo");
	}

	return new_pred;
}

void
ScalarDataGen::gen_extra(clite::StmtList& stmts, ExprPtr& e) 
{
	const auto& n = e->fun;
	clite::Factory factory;

	bool is_global_aggr = false;
	e->is_aggr(&is_global_aggr);
	if (is_global_aggr) {
		clite::StmtList statements;

		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		const std::string tpe(e->props.type.arity.size() > 0 ?
			e->props.type.arity[0].type : std::string(""));

		std::string agg_type;
		std::string comb_type;
		if (n == "aggr_gcount") {
			agg_type = "COUNT";
			comb_type = "SUM";
		} else if (n == "aggr_gsum") {
			agg_type = "SUM";
			comb_type = "SUM";
		} else {
			ASSERT(false);
		}

		auto acc = new_global_aggregate(tpe, tbl, col, comb_type);

		clite::ExprPtr arg;

		if (n == "aggr_gcount") {
			arg = factory.literal_from_str("0");
		} else {
			arg = factory.reference(expr2get0(e->args[1]));
		}

		statements.emplace_back(factory.effect(
			factory.function("AGGR_" + agg_type,
				factory.reference(acc), arg
			)
		));
		if (e->pred) {
			auto pred = get(e->pred);
			ASSERT(pred);
			stmts.push_back(factory.predicated(
				factory.reference(pred->var), statements));
		} else {
			stmts.push_back(factory.scope(statements));
		}

		return;
	}
}

DataGenBufferPosPtr
ScalarDataGen::write_buffer_get_pos(const DataGenBufferPtr& buffer,
	clite::Block* flush_state, const clite::ExprPtr& num,
	const DataGenExprPtr& pred, const std::string& dbg_flush)
{
	(void)num;
	clite::Factory f;
	return DataGen::write_buffer_get_pos(buffer, flush_state,
		f.literal_from_int(1), pred, dbg_flush);
}

DataGenBufferPosPtr
ScalarDataGen::read_buffer_get_pos(const DataGenBufferPtr& buffer,
	const clite::ExprPtr& num, clite::Block* empty, const std::string& dbg_refill)
{
	(void)num;
	clite::Factory f;
	return DataGen::read_buffer_get_pos(buffer, f.literal_from_int(1), empty,
		dbg_refill);
}

DataGenBufferColPtr
ScalarDataGen::write_buffer_write_col(const DataGenBufferPosPtr& pos,
	const DataGenExprPtr& expr, const std::string& dbg_name)
{
	printf("write_buffer state=%p\n", m_codegen.get_current_state());

#if 0
	printf("cid=%s\n", expr->get_c_id().c_str());
	printf("type=%s\n", expr->var_type.c_str());
#endif

	auto buf_var = get_fragment().new_var(unique_id(), expr->var->type + "*",
		clite::Variable::Scope::ThreadWide);

	auto column = std::make_shared<DataGenBufferCol>(pos->buffer, 
		buf_var, expr->var->type, expr->predicate);

	buffer_ensure_col_exists(column, expr->var->type);

	LOG_DEBUG("ScalarDataGen::write_buffer_write_col: dbg_name=%s, type=%s\n",
		dbg_name.c_str(), expr->var->type.c_str());


	pos->buffer->columns.push_back(column);

	clite::Builder builder(*m_codegen.get_current_state());

	builder
		<< builder.comment("write buffer '" + dbg_name + "'")
		<< builder.assign(builder.array_access(
				builder.reference(column->var),
				builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var))),
			builder.reference(expr->var))
		;

	if (expr->var->type == "pred_t") {
		builder
			<< builder.log_debug("write buf tpe %s from %s to %s at %d (%p) with %d", {
				builder.str_literal(expr->var->type),
				builder.function("STRINGIFY", builder.literal_from_str(expr->var->name)),
				builder.function("STRINGIFY", builder.literal_from_str(column->var->name)),
				builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var)),
				builder.function("ADDRESS_OF", builder.array_access(builder.reference(column->var), builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var)))),
				builder.reference(expr->var)});

	}

	return column;
}

DataGenExprPtr
ScalarDataGen::read_buffer_read_col(const DataGenBufferPosPtr& pos,
	const DataGenBufferColPtr& column, const std::string& dbg_name)
{
	auto& state = *m_codegen.get_current_state();
	std::ostringstream impl;

	printf("read_buffer state=%p\n", m_codegen.get_current_state());

	const std::string tpe(column->scal_type);

	auto ptr = std::make_shared<ScalarExpr>(*this,
		get_fragment().new_var(unique_id(), tpe,
			clite::Variable::Scope::Local),
		column->predicate);

	clite::Builder builder(state);


	LOG_DEBUG("ScalarDataGen::read_buffer_read_col: dbg_name=%s, type=%s into=%s\n",
		dbg_name.c_str(), tpe.c_str(), ptr->var->name.c_str());

	builder
		<< builder.comment("read_buffer '" + column->var->name + "'" +
			" into '" + dbg_name + "'")
		<< builder.assign(ptr->var,
			builder.array_access(
				builder.reference(column->var),
					builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var))));

#ifdef TRACE
	if (true || tpe == "pred_t") {
		builder
			<< builder.log_error("%s: %s: read buf tpe %s from %s to %s at %d (%p) with %d end", {
				builder.literal_from_str("g_pipeline_name"), builder.literal_from_str("g_buffer_name"),
				builder.str_literal(tpe),
				builder.function("STRINGIFY", builder.literal_from_str(column->var->name)),
				builder.function("STRINGIFY", builder.literal_from_str(ptr->var->name)),
				builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var)),
				builder.function("ADDRESS_OF", builder.array_access(builder.reference(column->var),builder.function("BUFFER_CONTEXT_OFFSET", builder.reference(pos->var)))),
				builder.reference(ptr->var)});
	}
#endif
	return ptr;
}

void
ScalarDataGen::prefetch(const ExprPtr& e, int temporality)
{
	bool match = false;
	const auto& n = e->fun;

	std::ostringstream impl;

	bool table_in = false;
	bool table_out = false;

	ASSERT(e->is_table_op(&table_out, &table_in));
	std::string tbl, col;
	e->get_table_column_ref(tbl, col);

	const std::string prefetch_data("PREFETCH_DATA" + std::to_string(temporality));

	clite::Builder builder(*m_codegen.get_current_state());

	clite::StmtPtr stmt;
	if (!match && !n.compare("bucket_lookup")) {
		auto index = expr2get0(e->args[1]);

		auto hash_mask = get_hash_mask_var(tbl);
		auto hash_index = get_hash_index_var(tbl);

		stmt = builder.effect(builder.function(prefetch_data,
				builder.function("ADDRESS_OF",
					builder.array_access(
						builder.reference(hash_index),
						builder.function("&", builder.reference(index), builder.reference(hash_mask))
					))));

		match = true;
	}

	if (!match && !n.compare("bucket_next")) {
		stmt = builder.effect(builder.function(prefetch_data,
				// builder.function("ADDRESS_OF",
					access_column(tbl, "next", get_bucket_index_expr(e))));

		match = true;
	}

	if (!match) {
		stmt = builder.effect(builder.function(prefetch_data,
				builder.function("ADDRESS_OF",
					access_column(tbl, e->props.first_touch_random_access, get_bucket_index_expr(e)))));

		match = true;
	}

	ASSERT(match);


	// builder << builder.log_debug("prefetch", {});
	if (e->pred) {
		builder << builder.predicated(
				builder.reference(get(e->pred)->var),stmt
			);
	} else {
		builder << stmt;
	}
}

std::string
ScalarDataGen::get_flavor_name() const
{
	return "scalar";
}
