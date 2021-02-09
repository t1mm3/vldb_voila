#include "cg_vector.hpp"
#include "codegen.hpp"
#include "voila.hpp"
#include "utils.hpp"

#define EOL std::endl

VectorCodegen::VectorCodegen(QueryConfig& config) : Codegen(config) {
	push_driven = true;
}

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


template<typename T>
static void goto_case(T& o, i64 state)
{
	if (state >= 0) {
		o << "        state = " << state << ";" << std::endl;
	}
}

template<typename T>
static void end_case(T& o)
{
	o
		<< "       } /* case */" << std::endl
		<< "        break;" << std::endl;
}

template<typename T>
static void end_case(T& o, i64 state)
{
	goto_case(o, state);
	end_case(o);
}

template<typename T>
static void new_case(T& o, i64 state)
{
	o
		<< "      case " << state << ":" << std::endl
		<< "		LOG_TRACE(\"entering case " << state << " at %d\\n\", __LINE__);" << std::endl
		<< "       {" << std::endl;
}

void
VectorCodegen::evaluate(const std::string& n, Expression& expr,
	Expression* pred, bool effect, const std::string& num,
	const std::string& reason)
{
	if (num.size() > 0) {
		decl << "VecScalar<sel_t> " << num << "; /* num */" << std::endl;
		init_scalar(init, num, "sel_t");
		next << num << ".set_value(";
	}
	next << n << ".evaluate(";
	if (pred) {
		next << expr2get0(pred) << ", " << expr2num[pred] << ".value, ";
	} else {
		// TODO: derive 'num'
		next << "(sel_t*)nullptr, ";

#if 1
		if (expr.props.vec_cardinality) {
			auto num = expr2num.find(expr.props.vec_cardinality.get());
			ASSERT(num != expr2num.end());
			next << num->second << ".value";
		} else {
			next << "query.config.vector_size";
		}
#else
		next << "query.config.vector_size";
#endif
		next << ", ";
	}
	next << "this, current_evaluator" << ")";

	if (num.size() > 0) {
		next << ")";
	}
	next << "; /*" << reason << "*/" << std::endl;
}

void
VectorCodegen::gen(GenCtx& ctx, Expression& e)
{
	// printf("pre fun = %s\n", e.fun.c_str());
	// materialize all variables
	if (e.props.gen_recurse.size() > 0) {
		for (size_t c=0; c<e.args.size(); c++) {
			auto& child = e.args[c];
			if (e.props.gen_recurse[c] && expr2var.find(child.get()) == expr2var.end()) {
				gen(ctx, *child);
			}
		}
	} else {
		for (auto& child : e.args) {
			if (expr2var.find(child.get()) == expr2var.end()) {
				gen(ctx, *child);
			}
		}
	}	

	if (e.pred && expr2var.find(e.pred.get()) == expr2var.end()) {
		gen(ctx, *e.pred);
	}

	auto new_vec = [&] (auto type) {
		std::string id(unique_id());
		decl << "Vec<" << translate_type(type) << "> " << id << ";" << std::endl;
		init << "," << id << "(\"" << id << "\", p.query)" << std::endl;
		return id;
	};

	auto new_this_vec = [&] () { return new_vec(e.props.type.arity[0].type); };


	auto new_expr2 = [&] (const std::string& expr_type, const std::string& type,
			const std::string& val, const std::string& args) {
		std::string id(unique_id());
		decl << expr_type << " " << id << ";" << std::endl;
		init << "," << id << "(\"" << id <<"\", this, \"" << type << "\", " << val;
		if (args.size() > 0) {
			init << ", " << args;
		}
		init << ")" << std::endl;
		init_body << "add_resetable(&"<< id << ");" << std::endl;
		return id;
	};


	auto new_expr = [&] (const std::string& expr_type, const std::string& type,
			const std::string& val, const std::string& args) {
		return new_expr2(expr_type, type, "\"" + val + "\"", args);
	};

	if (!e.fun.compare("scan")) {
		auto& child = e.args[1];
		std::string tbl, col;
		e.get_table_column_ref(tbl, col);

		auto vec = new_this_vec();
		auto id = new_expr2("VecWrap", translate_type(e.props.type.arity[0].type), "&" + vec, "");

		ASSERT(tbl.size() > 0);
		ASSERT(col.size() > 0);
		next << vec << ".first = thread." << tbl << "->col_" << col << ".get(" << expr2get0(child) << ".value.offset);" << std::endl;

		expr2num[&e] = expr2num[child.get()];
		expr2set0(&e, id);
		return;
	}

	if (!e.fun.compare("selvalid")) {
		auto& child = e.args[0];

		auto id = unique_id();

		decl << "VecScalar<pos_t> " << id << ";" << std::endl;
		init_scalar(init, id, "pos_t");
		next << id << ".set_value(IS_VALID(" << expr2get0(child.get()) << ".value)); /* selvalid */" << std::endl;

		expr2num[&e] = expr2num[child.get()];
		expr2set0(&e, id);
		return;
	}

	if (e.is_get_morsel()) {
		const auto& ref = e.args[0]->fun;
		const std::string context_name("_" + ref + "_ctx");


		decl
			<< "#ifndef MorselContext_decl_" << context_name << EOL
			<< "#define MorselContext_decl_" << context_name << EOL
			<< "MorselContext " << context_name << ";" << EOL
			<< "#endif" << EOL;

		init
			<< "#ifndef MorselContext_init_" << context_name << EOL
			<< "#define MorselContext_init_" << context_name << EOL
			<< "," << context_name << "(p)" << EOL
			<< "#endif" << EOL;

		init_body << "add_resetable(&"<< context_name << ");" << std::endl;

		auto id = unique_id();
		std::string num_id(unique_id());

		decl << "VecScalar<Morsel> " << id << ";" << std::endl;
		init << "," << id << "(\"" << id << "\", this, \"" << "Morsel" << "\")" << std::endl;

		decl << "VecScalar<pos_t> " << num_id << ";" << std::endl;
		init << "," << num_id << "(\"" << num_id << "\", this, \"" << "pos_t" << "\")" << std::endl;

		next << "thread." << ref << "->get_" << e.fun << "("<< id << ".value, "
			<< context_name << ");" << std::endl;
		next << num_id << ".set_value(" << id << ".value._num);" << EOL;

		expr2num[&e] = num_id;
		expr2set0(&e, id);

		return;
	}

	const bool is_write_pos = !e.fun.compare("write_pos");

	if (e.is_get_pos()) {
		const std::string id(unique_id());
		const std::string num_id(unique_id());
		decl << "VecScalar<Position> " << id << "; /* " << e.fun << " */" << EOL;
		init << "," << id << "(\"" << id << "\", this, \"" << "Position" << "\")" << EOL;

		decl << "VecScalar<pos_t> " << num_id << "; /* " << e.fun << " */" << EOL;
		init << "," << num_id << "(\"" << num_id << "\", this, \"" << "pos_t" << "\")" << EOL;

		const auto ref = !is_write_pos ? expr2get0(e.args[0]) : e.args[0]->fun;
		const std::string context_name("_" + ref + "_ctx");
		if (is_write_pos) {
			decl
				<< "#ifndef ThreadView_" << context_name << EOL
				<< "#define ThreadView_" << context_name << EOL
				<< "ITable::ThreadView " << context_name << ";" << EOL
				<< "#endif" << EOL;

			init
				<< "," << context_name << "(*thread." << ref << ", pipeline)" << EOL;
		}

		std::string num("query.config.vector_size");
		if (e.args.size() > 1) {
			ASSERT(e.args.size() == 2);
			num = expr2num[e.args[1].get()] + ".value";
		}

		next << id << ".set_value(";

		if (is_write_pos) {
			next << context_name;
		} else {
			next << ref << ".value";
		}

		next << ".get_" << e.fun << "(" << num << ", __func__, __FILE__, __LINE__));" << EOL;
		next << num_id << ".set_value(" << id << ".value.num);" << EOL;

		expr2num[&e] = num_id;
		expr2set0(&e, id);
		return;
	}

	if (!e.fun.compare("tget")) {
		ASSERT(e.args[0]->type == Expression::Type::LoleArg);
		std::string id(unique_id());

		auto tidx = TupleGet::get_idx(e);

		decl << "VecExpr* " << id << ";" << std::endl;
		init << "," << id << "(child->produced_columns[" << tidx << "])" << std::endl;

		special_col[&e] = id;

		expr2num[&e] = expr2num[e.args[0].get()];
		expr2set0(&e, id);
		return;
	}
	if (!e.fun.compare("tappend")) {
		expr2set0(&e, "BLA");
		expr2num[&e] = expr2num[e.args[0].get()];
		return;
	}

	switch (e.type) {
	case Expression::Reference:
		{
			ASSERT(e.props.type.arity.size() == 1 && "Must be scalar");

			auto& top = local_vars.top();
			ASSERT(top.find(e.fun) != top.end());

			auto ref = top[e.fun];
			auto ref_val = ref.data_ref;
			auto ref_num = ref.num_ref;
			ASSERT(ref_num.size() > 0);

			// We forcefully also cast scalars into vectors

			std::string id;
			if (e.props.scalar) {
				expr2set0(&e, ref_val);
			} else {
				id = new_expr2("VecWrap", translate_type(e.props.type.arity[0].type), "nullptr", "");

				next << "assign_vector(" + id + ", " + ref_val + ");" << std::endl;
				expr2set0(&e, id);
			}
			expr2num[&e] = ref_num;
		}

		break;

	case Expression::Function:
		{
			ASSERT(e.props.type.arity.size() == 1 && "Must be scalar");

			bool table_out = false;
			bool table_in = false;
			bool column = false;
			bool table_op = e.is_table_op(&table_out, &table_in, &column);

			std::ostringstream args;
			args << "{";
			bool first = true;
			std::string tbl;
			std::string col;

			if (table_op) {
				size_t r;
				if (column) {
					r = e.get_table_column_ref(tbl, col);
					ASSERT(r == 2);
				} else {
					r = e.get_table_column_ref(tbl);
					ASSERT(r == 1);
				}

				if (column && table_in && !table_out) {
					ASSERT(col.size() > 0);

					std::string tpe = e.props.type.arity[0].type;
					if (!e.fun.compare("check")) {
						tpe = e.args[0]->props.type.arity[0].type;
					}
					// ASSERT(col.size() > 0);
					auto vec = new_expr2("VecWrap", translate_type(tpe),
						"&thread." + tbl + "->col_" + col,
						"");
					args << "&" << vec << ", ";
				}

				if (column) {
					ASSERT(col.size() > 0);
					auto offset = new_expr2("VecConst", "u64",
						"std::to_string((u64)&thread." + tbl + "->coldef_" + col + ")",
						"");

					args << "&" << offset;
				}

				if (!column) {
					auto arg = new_expr2("VecConst", "u64",
						"std::to_string((u64)thread." + tbl + ")",
						"");

					if (!str_in_strings(e.fun, {
						"bucket_lookup", "bucket_next", "bucket_insert",
						"bucket_insert_done", "bucket_link", "bucket_build",
						"bucket_flush"
					})) {
						ASSERT(false && "todo");
					}

					args << "&" << arg;

					// add Pipeline for thread id
					if (!e.fun.compare("bucket_build")) {
						auto arg = new_expr2("VecConst", "u64",
							"std::to_string((u64)&pipeline)",
							"");
						args << ", &"
							<< arg;
					}
				}

				for (size_t i=1; i<e.args.size(); i++) {
					args << ", ";
					auto& a = e.args[i];
					if (special_col.find(a.get()) == special_col.end()) {
						args << "&" << expr2get0(a) << "/* normal expr */";
					} else {
						args << special_col[a.get()] << "/* input col */";
					}
				}
			} else {
				for (auto& a : e.args) {
					if (!first) {
						args << ", ";
					}
				
					if (special_col.find(a.get()) == special_col.end()) {
						args << "&" << expr2get0(a) << "/* normal expr */";
					} else {
						args << special_col[a.get()] << "/* input col */";
					}
					first = false;
				}
			}

			bool global_aggr = false;

			e.is_aggr(&global_aggr);

			if (global_aggr) {
				auto arg = new_expr2("VecConst", "u64",
					"std::to_string((u64)&global_aggr_allocated)",
					"");
				args << ", &" << arg;

				arg = new_expr2("VecConst", "u64",
						"std::to_string((u64)thread." + tbl + ")",
						"");

				args << ", &" << arg;
			}

			args << "}";

			bool patch_func = table_op && column && table_out;
			if (patch_func) {
				args << ", false /* no alloc */";
			}

			auto id = !e.fun.compare("selunion") ?
				new_expr2("VecWrap", "u64", "nullptr", "") :
				new_expr("VecFunc", translate_type(e.props.type.arity[0].type), e.fun,
				args.str());

			if (patch_func) {
				ASSERT(col.size() > 0);
				init_body << id << ".vec = &thread." << tbl + "->col_" << col << "; /* table out */" << std::endl;
			}
			if (global_aggr) {
				next << "if (!global_aggr_allocated) {"
					<< "  auto table = " << "thread." << tbl << ";" << EOL
					<< "  Block* block = table->hash_append_prealloc<false>(1);" << EOL
					<< "  void** buckets = table->get_hash_index();" << EOL
					<< "  char* new_bucket = block->data + (block->width * block->num);" << EOL
					<< "  buckets[0] = new_bucket;" << EOL
					<< "  table->hash_append_prune<false>(block, 1);" << EOL
					<< "  global_aggr_allocated = (void*)new_bucket; " << EOL
					<< "  LOG_TRACE(\"global_aggr_allocated = %p\\n\", global_aggr_allocated);"
					<< "}"
					;

#if 0
				const auto& acc = unique_id();
				const auto& acc_type = translate_type(e.props.type.arity[0].type);
				decl << "VecScalar<" << acc_type << "> " << acc << "; /* " << e.fun <<" */" << std::endl;
				init_scalar(init, acc, acc_type);

				auto &arg = *e.args[1];

				const auto arg_type = arg.props.type.arity[0].type; 
				std::string sel;
				std::string num;
				if (e.pred) {
					sel = expr2get0(e.pred);
					num = expr2num[e.pred.get()] + ".value";
				} else {
					sel = "nullptr";
					num = const_vector_size();
				}

				evaluate(id, arg, e.pred.get(), false, "", e.fun);

				if (!e.fun.compare("aggr_gsum")) {
					next << "aggr_gsum__" << acc_type << "__" << arg_type << "_col"<< "("
						<< sel << ","
						<< num << ","
						<< "&" << acc << ".value, "
						<< "get_vector(" << expr2get0(e.args[1]) << ")->first);" << std::endl;
				} else if (!e.fun.compare("aggr_gcount")) {
					ASSERT(false && "todo");
				} else {
					ASSERT(false);
				}
#endif
			}

			if (e.is_select()) {
				const auto& num = unique_id();
				if (!e.fun.compare("selunion")) {
					auto left = e.args[0];
					auto right = e.args[1];

					// TODO: must be selects anyway, we skip eval() for now

					decl << "VecScalar<sel_t> " << num << "; /* num */" << std::endl;
					init_scalar(init, num, "sel_t");
					next << num << ".set_value(" << std::endl
						<< "ComplexFuncs::selunion((sel_t*)(get_vector(" << id << ")->first)," << std::endl
						<< "(sel_t*)(get_vector(" << expr2get0(left) << ")->first), " << std::endl
						<< expr2num[left.get()] << ".value," << std::endl
						<< "(sel_t*)(get_vector(" << expr2get0(right) << ")->first), " << std::endl
						<< expr2num[right.get()] << ".value));"  << std::endl;
				} else {
					// evaluate
					evaluate(id, e, e.pred.get(), false, num, e.fun);
				}
				expr2num[&e] = num;
			} else  {
				if (e.pred) {
					const auto& num = expr2num[e.pred.get()];
					ASSERT(num.size() > 0);
					expr2num[&e] = num;
				} else {
					expr2num[&e] = const_vector_size();
				}				
			}

			expr2set0(&e, id);
		}

		break;
	case Expression::Constant:
		{
			ASSERT(e.props.type.arity.size() == 1 && "Must be scalar");

			const auto& num = const_vector_size();
			auto id = new_expr("VecConst", translate_type(e.props.type.arity[0].type), e.fun, "");

			expr2num[&e] = num;
			expr2set0(&e, id);
			break;			
		}

		break;

	case Expression::Type::LolePred:
		expr2num[&e] = "in_num";
		special_col[&e] = "in_sel";
		expr2var[&e] = {special_col[&e]};
		break;

	case Expression::Type::LoleArg:
		{
			std::string num(unique_id());
			expr2num[&e] = num;
			expr2var[&e] = lolearg;
		}
		break;

	default:
		ASSERT(false);
		break;
	}

}

void
VectorCodegen::gen(GenCtx& ctx, Statement& e)
{
	if (e.pred && expr2var.find(e.pred.get()) == expr2var.end()) {
		gen(ctx, *e.pred);
	}

	if (e.expr && expr2var.find(e.expr.get()) == expr2var.end()) {
		gen(ctx, *e.expr);
	}

	switch (e.type) {
	case Statement::Type::Loop: {
			ASSERT(e.expr);

			// loop body
			auto loop_body_case = ++ctx.current_case;
			auto loop_epi_case = ++ctx.current_case;


			next << "/* loop refs: */" << std::endl;

			std::unordered_map<std::string, LocalVar> ref_map;
			for (auto& p : e.props.loop_refs.ref_strings) {

				// does reference exist
				auto& top = local_vars.top();
				if (top.find(p) != top.end()) {
					auto& source_var = top[p];
					next << "/* '" << p << "' " << "variable from '" << source_var.data_ref << "'";

					bool scalar = source_var.scalar;
					if (scalar) {
						next << " directly */" << std::endl;
						ref_map[p] = source_var;
					} else {
						auto id = unique_id();
						next << " as '" << id << "' */" << std::endl;
						ref_map[p] = {id, source_var.num_ref, scalar};

						decl << "VecWrap " << id << ";" << std::endl;
						init << ", " << id << "(\"" << id << "\", this, " << source_var.data_ref << ")" << std::endl;

						next << "assign_vector(" << id << ", " << source_var.data_ref << ");" << std::endl;
					}
					next << std::endl;
				}
			}

			end_case(next, loop_body_case);
			new_case(next, loop_body_case);

			auto loop_eval = unique_id();
			// next << loop_eval << ".eval_round = 0;" << std::endl;
			decl << "VecEvaluator " << loop_eval << ";" << std::endl;
			init << "," << loop_eval << "(\"" << loop_eval << "\")" << std::endl;
			init_body << "add_resetable(&" << loop_eval << ");" << std::endl;

			// add new local variables
			// local_vars.push(ref_map);

			// evaluate predicate 
			next << "{ /* loop check */" << std::endl
				<< "  " << loop_eval << ".eval_round++;" << std::endl
				<< "  current_evaluator = &" << loop_eval << ";" << std::endl
				<< "  const bool p = ";
			if (e.expr) {
				if (e.expr->props.scalar) {
					next << expr2get0(e.expr) << ".value; /* scalar */";
				} else {
					next << expr2num[e.expr.get()] << ".value > 0; /* non-scalar */";
				}

				next << std::endl;
			}

			next << "  if (!p) { LOG_TRACE(\"Loop check failed @ %d\\n\", __LINE__); state = " << loop_epi_case << "; break; }" << std::endl
				<< "} /* loop check */ " << std::endl;

			for (auto& stmt : e.statements) {
				gen(ctx, *stmt);
			}

			// local_vars.pop();

			// next << "state = " << loop_body_case << "; /* goto loop body */" << std::endl;

			// loop epilogue: step over to new coroutine
			end_case(next, loop_body_case);
			new_case(next, loop_epi_case);
		}
		break;

	case Statement::Type::Assignment:
		{
			const std::string varname(e.var);
			bool scalar = e.expr->props.scalar;
			bool predicate = e.expr->props.type.category == TypeProps::Category::Predicate;
			auto& top = local_vars.top();
			bool initial = false;
			if (top.find(varname) == top.end()) {
				initial = true;
				ASSERT(e.props.type.arity.size() == 1 && "must be scalar function");
				const auto id = unique_id();
				const auto num = unique_id();
				next << "/* new local variable '" << e.var << "' as '"
					<< id << "' */" << std::endl;
				top[varname] = {id, num, scalar};

				const auto& type = translate_type(e.props.type.arity[0].type);

				const auto& comment = "/* assign */";
				if (scalar) {
					decl << "VecScalar<" << type << "> " << id << ";" << comment << std::endl;
					init << "," << id << "(\"" << id << "\", this, \"" << type << "\")" << std::endl;
				} else {
					decl << "VecWrap " << id << ";" << comment << std::endl;
					init << "," << id << "(\"" << id << "\", this, \"" << type << "\")" << std::endl;
				}
				decl << "VecScalar<sel_t> " << num << "; /* num */" << std::endl;
				init_scalar(init, num, "sel_t");
				next << "/* new var variable '" << varname << "' aka '" << id << "' */" << std::endl;
			} else {
				const auto& id = top[varname].data_ref;
				next << "/* updating variable '" << varname << "' as '" << id << "' */" << std::endl;
			}

			const auto& id = top[varname].data_ref;
			const auto& num = top[varname].num_ref;
			std::string prefix(initial ? "initial" : "update");

			if (scalar) {
				next << id << ".set_value(" << expr2get0(e.expr) << ".value); /* " << prefix << " assign '" << varname << "' scalar */" << std::endl;
			} else {
				if (!predicate) {
					evaluate(expr2get0(e.expr), *e.expr, e.expr->pred.get(), false, "", "assign");
				}
				// ASSERT(false && "not sure whether this will always work");
				next << "assign_vector(" << id << ", " << expr2get0(e.expr) << "); /* " << prefix << " assign '" << varname << "' vec */" << std::endl;
			}

			auto old_num = expr2num[e.expr.get()];
			if (old_num.size() > 0) {
				next << num << ".set_value(" << old_num << ".value); /* " << prefix << " assign '" << varname << "' num */" << std::endl;
			} else {
				next << "/* has no 'num' */" << EOL;
			}
		}

		break;
	case Statement::Type::EffectExpr:
		{
			auto n = expr2get0(e.expr);

			evaluate(n, *e.expr, e.expr->pred.get(), true, "", "effect");
		}
		break;

	case Statement::Type::Emit:
		ASSERT(e.expr->fun == "tappend" && "Not supported yet");
		lolearg = expr2var[e.expr.get()];
		// ASSERT(lolearg.size() > 0);
		if (e.pred) {
			next << "out_sel = ";
			if (special_col.find(e.pred.get()) == special_col.end()) {
				next << "&";
			}
			next << expr2get0(e.pred) << ";" << std::endl;
			next << "out_num = " << expr2num[e.pred.get()] << ".value;" << std::endl;
		} else {
			// ASSERT(expr2num.find(e.expr) != expr2var.end());
			next << "out_sel = nullptr;" << std::endl;
			if (expr2num.find(e.expr.get()) == expr2num.end()) {
				next << "out_num = query.config.vector_size;" << std::endl;
			} else {
				next << "out_num = " << expr2num[e.expr.get()]<< ".value;" << std::endl;
			}
		}

		ctx.current_case++;
		next << "LOG_TRACE(\"%s: out_sel=%p out_num=%d in round %d\\n\", LOLEPOP_DBG_NAME, out_sel, out_num, current_evaluator ? current_evaluator->eval_round : -1); " << std::endl;
		next << "  if (out_num > 0) {" << std::endl
			<< "     produced_sel = out_sel;" << std::endl
			<< "     evaluate_produced_columns(out_num, out_sel, current_evaluator);" << std::endl;
		goto_case(next, ctx.current_case);
		next
			<< "     return out_num;" << std::endl
			<< "   } /* out_num > 0 */" << std::endl;
		goto_case(next, ctx.current_case);


		// connect these expressions with produced columns
		{
			int i=0;
			for (auto& c : e.expr->args) {
				ASSERT(c);

				if (c->type == Expression::Type::LoleArg) {
					init_body << "for (auto& ccol : child->produced_columns) produced_columns.push_back(ccol);" << std::endl;
				} else {
					init_body << "produced_columns.push_back(";
					if (special_col.find(c.get()) == special_col.end()) {
						init_body << "&";
					}
					init_body << expr2get0(c)
						<< ")";
				}

				init_body << ";" << std::endl;
				i++;
			}
		}

		// step over to new coroutine
		end_case(next, -1);
		new_case(next, ctx.current_case);
		break;

	case Statement::Type::Done:
		next << "return 0; /* Done */" << std::endl;
		break;

	case Statement::Type::MetaStmt:
		break;

	default:
		ASSERT(false);
		break;
	}
}

void
VectorCodegen::gen_lolepop(Lolepop& l, Pipeline& p)
{
	num_vector_size = "";
	decl << "#include \"kernels.hpp\"" << std::endl;

	auto debug_macros = [&] (auto& out, bool begin) {
		if (begin) {
			out << "#define LOLEPOP_DBG_NAME \"" << l.name << "\"" << std::endl;
		} else {
			out << "#undef LOLEPOP_DBG_NAME" << std::endl;
		}
	};
	
	debug_macros(decl, true);
	debug_macros(init, true);
	debug_macros(next, true);

	decl << "struct ThisThreadLocal;" << std::endl;
	decl << "struct Lolepop_" << l.name	<< " : VecOp {" << std::endl
		<< "  void init() final {};" << std::endl
		<< "  sel_t next() final;" << std::endl
		<< "  ThisThreadLocal& thread;" << std::endl
		<< "  Lolepop_" << l.name << "(VecOp* child, IPipeline& p);" << std::endl
		<< "VecScalar<sel_t> in_num;" << std::endl;

	init << "  Lolepop_" << l.name << "::Lolepop_" << l.name << "(VecOp* child, IPipeline& p) : VecOp(child, p, LOLEPOP_DBG_NAME) " << std::endl
		<< ", in_num(\"in_num\", this, \"sel_t\"), thread(query.get_thread_local<ThisThreadLocal>(p))" << std::endl; 

	next << "sel_t Lolepop_" << l.name << "::next() {" << std::endl
		<< "    while (1) {" << std::endl
		// << "  do {" << std::endl
		<< "      switch (state) {" << std::endl;
		
	new_case(next, 0);
	next
		<< "        if (child) {" << std::endl
		<< "          in_num.set_value(child->get_next());" << std::endl
		<< "          in_sel = child->produced_sel;" << std::endl
		<< "          if (!in_num.value) {" << std::endl
		<< "            LOG_TRACE(\"Done at " << l.name << "\\n\");" << std::endl
		<< "            return 0;" << std::endl
		<< "          }" << std::endl
		<< "        } else {" << std::endl
		<< "          in_num.set_value(query.config.vector_size);" << std::endl
		<< "          in_sel = nullptr;" << std::endl
		<< "        }" << std::endl
		<< "        current_evaluator = &op_evaluator; op_evaluator.eval_round++;" << std::endl;

	for (auto stmt : l.statements) {
		GenCtx ctx;
		gen(ctx, *stmt);
	}
	end_case(next, 0); // go back to state=0
	next
		<< "      default: ASSERT(false); break;" << std::endl
		<< "      } /* switch */" << std::endl
		<< "    } /* while */" << std::endl
		<< "  return 0;" << std::endl;

	decl << "} /* " << l.name << " */;" << std::endl;

	init << "{" << std::endl;
	init << clear_stream(init_body);
	init << "}" << std::endl;

	next << "}" << std::endl;

	debug_macros(decl, false);
	debug_macros(init, false);
	debug_macros(next, false);

	Lolepop* n = move_to_next_op(p);
	if (n) {
		wrap_lolepop(l, [&] () {
			gen_lolepop(*n, p);
		});
	}
}


void
VectorCodegen::gen_pipeline(Pipeline& p, size_t number)
{
	auto& ls = p.lolepops;

	lolepop_idx = 0;

	auto& l= *ls[lolepop_idx];

	wrap_lolepop(l, [&] () {
		gen_lolepop(l, p);
	});

	decl << "struct Pipeline_" << number << " : VecPipeline {" << std::endl;
	for (auto& l : p.lolepops) {
		decl << " Lolepop_" << l->name << " _Lolepop_" << l->name << ";" << std::endl;
	}

	decl << " Pipeline_" << number << "(Query& q, size_t this_thread) : VecPipeline(q, this_thread) " << std::endl;

	std::string sink;

	for (auto& l : p.lolepops) {
		decl << ", " << "_Lolepop_" << l->name << "(";
		if (sink.size() > 0) {
			decl << "&_Lolepop_" << sink;
		} else {
			decl << "nullptr";
		}
		decl << ", *this)";
		sink = l->name;
	}
	decl << "{" << std::endl;
#if 0
		 << "last = ";

	if (last_pipeline) {
		decl << "true";
	} else {
		decl << "false";
	}
	decl << ";" << std::endl
#endif
	decl << "sink = &_Lolepop_" << sink << ";" << std::endl;
	for (auto& l : p.lolepops) {
		decl << "_Lolepop_" << l->name << ".init();" << std::endl;
	}
	decl << "}" << std::endl
		 << "}; /*Pipeline_" << number << "*/" << std::endl;

}
