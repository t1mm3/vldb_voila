#include "cg_hyper.hpp"
#include "codegen.hpp"
#include "voila.hpp"
#include "utils.hpp"
#include "runtime_utils.hpp"
#include "runtime_framework.hpp"

#define EOL "\n"

CodeCollector::CodeCollector(std::ostringstream& output)
	: output(output)
{
}

void
CodeCollector::flush()
{
	LOG_TRACE("CodeCollector::flush()\n");
	if (collapse_same_predicates) {
		buffer << last_predicate_epilogue;
		last_predicate_epilogue = "";
		last_predicate_prologue = "";

	}
	output << clear_stream(buffer);
}

CodeCollector::~CodeCollector()
{
	flush();
	// ASSERT(false);
}

void
CodeCollector::add_predicated_code(const std::string& prologue,
		const std::string& code, const std::string& epilogue) {
	if (!collapse_same_predicates) {
		buffer << prologue << code << epilogue;
		return;
	}
	bool different_mode = insert_mode != InsertMode::PredicatedCode;
	bool different_prologue = last_predicate_prologue.compare(prologue);
	bool different_epilogue = last_predicate_epilogue.compare(epilogue);

	LOG_TRACE("CodeCollector::add_predicated_code: diff_mode=%d, diff_pro=%d, "
			"diff_epi=%d, prologue='%s', code='%s', epilogue='%s'\n\n",
		different_mode, different_prologue, different_epilogue, 
		prologue.c_str(), code.c_str(), epilogue.c_str());

	if (different_mode || different_prologue || different_epilogue) {
		// mismatch
		flush();

		buffer << prologue;
		insert_mode = InsertMode::PredicatedCode;

		buffer << "/* CodeColl flush */" << EOL;
	} else {
		buffer << "/* CodeColl combine */" << EOL;
	}

	buffer << code;

	last_predicate_prologue = prologue;
	last_predicate_epilogue = epilogue;
}

// Wraps predicated code
struct PredicateWrapper {
	PredicateWrapper(CodeCollector& out)
	 : output(out), already_generated(false) {
	}

	void set_predicate(const std::string& t) {
		condition = t;
	}

	template<typename T>
	PredicateWrapper& operator << (const T &t) {
		predicated << t;
		return *this;
	}

	void close() {
		if (already_generated) {
			return;
		}
		already_generated = true;

		std::string code = predicated.str();

		if (code.size() == 0) {
			return;
		}

		std::string prologue;
		std::string epilogue;

		bool has_predicate = condition.size() > 0;
		if (has_predicate) {
			prologue = "if (" + condition + ") {" + EOL;
			epilogue = "}";
		}

		output.add_predicated_code(prologue, code, epilogue);
	}

	~PredicateWrapper() {
		close();
	}

private:
	std::ostringstream predicated;
	std::string condition;
	CodeCollector& output;
	bool already_generated;
};


HyperCodegen::HyperCodegen(QueryConfig& config, bool jit)
 : Codegen(config), code(next), jit(jit)
{
}

void
HyperCodegen::gen(GenCtx& ctx, Expression& e)
{
	bool first;

	if (e.pred) {
		if (expr2var.find(e.pred.get()) == expr2var.end()) {
			gen(ctx, *e.pred);
		}
	}
	
	// materialize all variables
	if (e.props.gen_recurse.size() > 0) {
		for (size_t c=0; c<e.args.size(); c++) {
			auto& child = e.args[c];
			if (e.props.gen_recurse[c] && 
				expr2var.find(child.get()) == expr2var.end()) {
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

	PredicateWrapper predicated(code);

	bool no_predicate_needed = e.is_select();

	if (e.pred && !no_predicate_needed) {
		predicated.set_predicate(expr2get0(e.pred));
	}

	bool table_in = false;
	bool table_out = false;



	if (e.is_table_op(&table_out, &table_in)) {
		std::string tbl, col;
		e.get_table_column_ref(tbl, col);

		auto get_row_type = [&] (const auto& tbl) {
			return "__struct_" + tbl + "::Row";
		};

		auto access_raw_column = [&] (const auto& tbl, const auto& col, const auto& index) {
			return "((" + get_row_type(tbl) + "*)" + index + ")->" + col;
		};

		auto access_column = [&] (const auto& tbl, const auto& col, const auto& index) {
			return access_raw_column(tbl, "col_" + col, index);
		};


		bool is_global_aggr = false;
		bool is_aggr = e.is_aggr(&is_global_aggr);
		if (is_global_aggr) {
			auto acc = unique_id();
			new_decl(e.props.type.arity[0].type, acc);

			if (!e.fun.compare("aggr_gcount")) {
				predicated << acc << "++;";
			} else if (!e.fun.compare("aggr_gsum")) {
				auto arg = expr2get0(e.args[1]);
				predicated << acc << " += " << arg << ";";
			} else {
				ASSERT(false);
			}

			if (!pipeline_end_gen) {
				pipeline_end
					<< "u64 pipeline_end_bucket_id = 0;" << EOL
					<< "{" << EOL
					<< "  auto table = " << "thread." << tbl << ";" << EOL
					<< "  Block* block = table->hash_append_prealloc<false>(1);" << EOL
					<< "  char* new_bucket = block->data + (block->width * block->num);" << EOL
					<< "  void** buckets = table->get_hash_index();" << EOL
					<< "  buckets[0] = new_bucket;" << EOL
					<< "  table->hash_append_prune<false>(block, 1);" << EOL
					<< "  pipeline_end_bucket_id = (u64)new_bucket; " << EOL
					// << "  printf(\"" << e.fun << ": index=%lu count=%lu bucket=%lu mask=%lu\\n\", index, table->count, table->buckets[index], table->bucket_mask);" << EOL
					<< "}" << EOL
					<< "LOG_TRACE(\"Bucket %p\\n\", pipeline_end_bucket_id);" << EOL
					;

				pipeline_end
					<< access_raw_column(tbl, "hash", "pipeline_end_bucket_id")
						<< " = " << kGlobalAggrHashVal << ";" << EOL;
			}

			pipeline_end_gen = true;
			pipeline_end << "{" << EOL
				<< "/* propagate " << e.fun << " to table */" << EOL 
				<< "LOG_TRACE(\"propagate %d\\n\", " << acc << ");" << EOL
				<< access_column(tbl, col, "pipeline_end_bucket_id") << " += " << acc << ";" << EOL
				<< "}" << EOL;
			expr2set0(&e, "AGGR-HAS-NO-RESULT");
			// ASSERT(false && "todo");
		} else if (is_aggr) {
			predicated << "{" << EOL;
			// predicated << "LOG_DEBUG(\"" << e.fun << "\\n\");" << EOL;
			// predicated << "auto &aggr = thread." << tbl << "->rows[" << expr2get0(e.args[1]) << "].col_" << col << ";" << EOL;
			predicated << "auto &aggr = " << access_column(tbl, col, expr2get0(e.args[1])) << ";" << EOL;


			if (!e.fun.compare("aggr_count")) {
				predicated << "aggr++;";
			} else {
				auto arg = expr2get0(e.args[2]);
				if (!e.fun.compare("aggr_sum")) {
					predicated << "aggr += " << arg << ";";
				} else if (!e.fun.compare("aggr_min")) {
					predicated << "if (" << arg << " < aggr) aggr = " << arg <<";";
				} else if (!e.fun.compare("aggr_max")) {
					predicated << "if (" << arg << " > aggr) aggr = " << arg <<";";
				} else {
					ASSERT(false);
				}
			}
			predicated << EOL;
			predicated << "}" << EOL;			

			// no result, must be effect
			predicated << "/* " << e.fun << "*/" << EOL;

			expr2set0(&e, "AGGR-HAS-NO-RESULT");
		} else {
			const auto id = unique_id();
			bool match = false;

			auto access_table = [] (const auto& tbl) {
				return "thread." + tbl;
			};


			if (!match && !e.fun.compare("gather")) {
				new_decl(e.props.type.arity[0].type, id);
				predicated << id << " = "
					<< access_column(tbl, col, expr2get0(e.args[1])) << ";" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("read")) {
				new_decl(e.props.type.arity[0].type, id);

				predicated << id << " = "
					<< expr2get0(e.args[1]) << ".current<" << get_row_type(tbl)
					<< ">()->col_" + col
					<< ";" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("check")) {
				new_decl(e.props.type.arity[0].type, id);
				predicated << "LOG_GEN_TRACE(\"" << e.fun << " at " << expr2get0(e.args[1]) << "\\n\");" << EOL;
				predicated << id << " = "
					<< access_column(tbl, col, expr2get0(e.args[1])) << "==" << expr2get0(e.args[2]) <<";" << EOL;
				predicated << "LOG_GEN_TRACE(\"%lld == %lld -> %lld\\n\", " << access_column(tbl, col, expr2get0(e.args[1])) << "," << expr2get0(e.args[2]) << "," << id << ");" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("write")) {
				new_decl(e.props.type.arity[0].type, id);

				predicated
					<< expr2get0(e.args[1]) << ".current<" << get_row_type(tbl)
					<< ">()->col_" + col
					<< " = "
					<< expr2get0(e.args[2])
					<< ";" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("scatter")) {
				// predicated << "LOG_GEN_TRACE(\"" << e.fun << " at " << expr2get0(e.args[1]) << " col[%lld]= %lld\\n\", (i64)" << expr2get0(e.args[1]) << ", " << "(i64)" << expr2get0(e.args[2]) << ");" << EOL;
				predicated << access_column(tbl, col, expr2get0(e.args[1])) << "=" << expr2get0(e.args[2]) <<";" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("bucket_lookup")) {
				std::string index = expr2get0(e.args[1]);
				new_decl(e.props.type.arity[0].type, id);
				predicated << id <<" = (u64)"
					<< access_table(tbl) << "->get_hash_index()[" << index << " & " << access_table(tbl) << "->get_hash_index_mask()];" << EOL;
				// predicated << "printf(\"" << e.fun << ": %p\\n\", " << id << ");" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("bucket_next")) {
				std::string index = expr2get0(e.args[1]);
				new_decl(e.props.type.arity[0].type, id);
				predicated << id <<" = " << access_raw_column(tbl, "next", index) << ";" << EOL;
				// predicated << "printf(\" next %p from index %p\\n\", " << id << "," << index << ");" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("bucket_insert")) {
				std::string index = expr2get0(e.args[1]);
				new_decl(e.props.type.arity[0].type, id);

				// TODO: impl
				predicated << "LOG_GEN_TRACE(\"" << e.fun << " at " << expr2get0(e.args[1]) << "\\n\");" << EOL;
				predicated << id << ";" << EOL
					<< "{" << EOL
					<< "  auto table = " << access_table(tbl) << ";" << EOL
					<< "  Block* block = table->hash_append_prealloc<false>(1);" << EOL
					<< "  char* new_bucket = block->data + (block->width * block->num);" << EOL
					<< "  auto row = (" << get_row_type(tbl) << "*)new_bucket;" << EOL
					<< "  const u64 index = (" << index << ") & table->get_hash_index_mask();" << EOL
					<< "  void** buckets = table->get_hash_index();" << EOL
					<< "  row->next = (u64)buckets[index];" << EOL
					// << "  printf(\"block data %p block num %llu new bucket %p old bucket %p\\n\", block->data, block->num, new_bucket, buckets[index]);" << EOL
					<< "  buckets[index] = new_bucket;" << EOL
					<< "  table->hash_append_prune<false>(block, 1);" << EOL
					<< "  " << id << " = (u64)new_bucket; " << EOL
					// << "  printf(\"" << e.fun << ": index=%lu count=%lu bucket=%lu mask=%lu\\n\", index, table->count, table->buckets[index], table->bucket_mask);" << EOL
					<< "}" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("bucket_build")) {
				new_decl(e.props.type.arity[0].type, id);
				predicated << access_table(tbl) << "->create_buckets(this);" << EOL;
				match = true;
			}

			if (!match && !e.fun.compare("bucket_flush")) {
				new_decl(e.props.type.arity[0].type, id);
				predicated << access_table(tbl) << "->flush2partitions();" << EOL;
				match = true;
			}

			ASSERT(match);

			expr2set0(&e, id);
		}

		return;
	}

	const auto id = unique_id();

	if (!e.fun.compare("scan")) {
		auto& child = e.args[1];

		std::string tbl, col;
		e.get_table_column_ref(tbl, col);

		new_decl(e.props.type.arity[0].type, id);
		predicated << id
			<<  "= thread." << tbl << "->col_" << col << "[" << expr2get0(child) << ".offset];" << EOL;

		expr2set0(&e, id);

		return;
	}

	if (e.is_get_morsel()) {
		const auto& ref = e.args[0]->fun;
		const std::string context_name("_" + ref + "_ctx");

		local_decl
			<< "#ifndef MorselContext_" << context_name << EOL
			<< "#define MorselContext_" << context_name << EOL
			<< "MorselContext " << context_name << "(*this);" << EOL
			<< "#endif" << EOL;

		new_decl("Morsel", id);
		// next << "if (!((ThisQuery&)(query))." << ref << "->get_" << e.fun << "("<< id << ", 1)) break;" << EOL;
		predicated << "thread." << ref << "->get_" << e.fun << "("<< id << ", "
			<< context_name << ");" << EOL;

		expr2set0(&e, id);

		return;
	}

	if (!e.fun.compare("write_pos")) {
		const auto& ref = e.args[0]->fun;

		const std::string context_name("_" + ref + "_ctx");

		local_decl
			<< "#ifndef ThreadView_" << context_name << EOL
			<< "#define ThreadView_" << context_name << EOL
			<< "ITable::ThreadView " << context_name << "(*thread." << ref << ", *this);" << EOL
			<< "#endif" << EOL;

		new_decl("Position", id);
		// next << "if (!((ThisQuery&)(query))." << ref << "->get_" << e.fun << "("<< id << ", 1)) break;" << EOL;
		predicated << id << " = " << context_name << ".get_" << e.fun << "(1, __func__, __FILE__, __LINE__);" << EOL;

		expr2set0(&e, id);

		return;
	}

	if (e.is_get_pos()) {
		auto& child = e.args[0];

		new_decl("Position", id);
		auto ref = expr2get0(child);
		// next << "if (!((ThisQuery&)(query))." << ref << "->get_" << e.fun << "("<< id << ", 1)) break;" << EOL;
		predicated << id << " = " << ref << ".get_" << e.fun << "(1, __func__, __FILE__, __LINE__);" << EOL;

		expr2set0(&e, id);
		return;
	}

#if 0
	if (e.is_get_pos()) {
		new_decl("pos_t", id);
		auto& ref = e.args[0]->fun;
		// next << "if (!((ThisQuery&)(query))." << ref << "->get_" << e.fun << "("<< id << ", 1)) break;" << EOL;
		predicated << "thread." << ref << "->get_" << e.fun << "("<< id << ", 1);" << EOL;

		expr2set0(&e, id);

		return;
	}
#endif
	if (jit) {
		if (expr2var.find(&e) != expr2var.end()) {
			return;
		}
	} else {
		ASSERT(expr2var.find(&e) == expr2var.end());
	}

	auto combine_pred = [&] (auto combine) {
		std::string r;
		if (!e.pred) {
			return r;
		}

		return combine + expr2get0(e.pred);
	};

	auto translate_infix_function = [&] () {
		auto& n = e.fun;

		std::string infix("");

		if (!n.compare("eq")) { infix = "=="; }
		else if (!n.compare("ne")) { infix = "!="; }
		else if (!n.compare("lt")) { infix = "<"; }
		else if (!n.compare("le")) { infix = "<="; }
		else if (!n.compare("gt")) { infix = ">"; }
		else if (!n.compare("ge")) { infix = ">="; }
		// else if (!n.compare("+")) { infix = "+"; }
		// else if (!n.compare("-")) { infix = "-"; }
		else if (!n.compare("and")) { infix = "&&"; }
		else if (!n.compare("or")) { infix = "||"; }
		else if (!n.compare("selunion")) { infix = "||"; }
		else if (!n.compare("add")) { infix = "+"; }
		else if (!n.compare("sub")) { infix = "-"; }
		else if (!n.compare("mul")) { infix = "*"; }

		if (infix.size() > 0) {
			ASSERT(e.args.size() == 2);
			predicated << expr2get0(e.args[0]) << " " << infix << " "<< expr2get0(e.args[1])
				<< "; /* " << e.fun << "*/" << EOL;
			return true;
		}

		return false;
	};

	auto translate_unary_function = [&] () {
		auto& n = e.fun;

		std::string prefix("");
		std::string postfix("");
		bool use_valid = false;

		auto cast2 = split(n, 'T');

		if (cast2.size() > 1 && !cast2[0].compare("cast")) {
			prefix = "(" + cast2[1] + ")";			
		} else if (!n.compare("not")) { 	prefix = "!"; }
		else if (!n.compare("sequence")) { 	prefix = "/*nothing*/"; }
		else if (!n.compare("sign")) { 		prefix = "-"; }
		else if (!n.compare("seltrue")) { 	prefix = "(!!"; postfix = ")" + combine_pred("&&"); }
		else if (!n.compare("selfalse")) { 	prefix = "(!"; 	postfix = ")" + combine_pred("&&"); }
		else if (!n.compare("selvalid")) { 	prefix = "IS_VALID("; postfix = ")"; use_valid = true; }
		else if (!n.compare("print")) { prefix = "printf(\"%p\", "; postfix = ")"; }

		if (prefix.size() > 0 || postfix.size() > 0) {
			ASSERT(e.args.size() == 1);
			predicated << prefix << expr2get0(e.args[0]) << postfix << "; /* " << e.fun << "*/" << EOL;
			return true;
		}

		return false;
	};

	auto translate_tuple_ops = [&] () {
		auto& n = e.fun;
		if (!n.compare("tappend")) {
			std::vector<std::string> r;
			for (auto& arg : e.args) {
				const auto& vec = expr2var[arg.get()];
				for (auto& v : vec) {
					r.push_back(v);
				}
			}
			predicated << "/* tappend */" << EOL;
			expr2var[&e] = r;
			return true;
		} else if (!n.compare("tget")) {
			const auto& vec = expr2var[e.args[0].get()];
			auto idx = TupleGet::get_idx(e);
			ASSERT(idx < vec.size());

			predicated << vec[idx] << "; /* tget " << idx << "*/" << EOL;
			expr2set0(&e, id);
			return true;
		}

		return false;
	};

	if (!e.fun.compare("tappend") || e.type == Expression::Type::LoleArg || e.type == Expression::Type::LolePred) {
	} else {
		ASSERT(e.props.type.arity.size() == 1 && "must be scalar");
	}

	std::string gen_fun_name(e.fun);

	switch (e.type) {
	case Expression::Type::Function:
		if (e.fun.compare("tappend")) {
			new_decl(e.props.type.arity[0].type, id);
			predicated << id <<" = ";
		}

		if (translate_tuple_ops()) {
			break;
		}
		expr2set0(&e, id);

		if (translate_infix_function()) {
			break;
		}
		if (translate_unary_function()) {
			break;
		}

		if (!e.fun.compare("hash") || !e.fun.compare("rehash")) {
			size_t idx = 0;
			if (!e.fun.compare("rehash")) {
				idx = 1;
			}
			gen_fun_name = "voila_hash<" + e.args[idx]->props.type.arity[0].type +
				">::" + e.fun;
		}

		predicated << gen_fun_name;
		predicated << "(" ;

		first = true;
		for (auto& child : e.args) {
			if (!first) {
				predicated << ", ";
			}
			predicated << expr2get0(child);
			first = false;
		}
		predicated << ")" << ";" << EOL;

		break;
	case Expression::Type::Constant:
		{
			const auto& type = e.props.type.arity[0].type;

			if (!type.compare("varchar")) {
				new_decl(type, id, "(\"" + e.fun + "\")", "const ");
			} else {
				new_decl(type, id, "(" + e.fun + ")", "const ");
			}
#if 0
			predicated << id <<" = ";
			predicated << e.fun << ";" << EOL;
#endif
			expr2set0(&e, id);

		}
		break;
	case Expression::Type::Reference:
		new_decl(e.props.type.arity[0].type, id);
		expr2set0(&e, id);

		{
			const std::string varname(e.fun);

			ASSERT(local_vars.size() > 0);
			ASSERT(local_vars.top().find(varname) != local_vars.top().end());
			auto& data_ref = local_vars.top()[varname].data_ref;

			predicated
				<< "LOG_GEN_TRACE(\"Reference '" << varname << "' (as u64: %lu) at %d\\n\", (u64)" << data_ref << ", __LINE__);" << EOL
				<< id <<" = "
				<< data_ref << ";"
				<< "/* ref '" << varname << "' */" << EOL;
		}
		break;
	case Expression::Type::LoleArg:
		expr2var[&e] = lolearg;
		predicated << "/* lolearg */ ";
		break;
	case Expression::Type::LolePred:
#if 0
		if (lolepred.size() > 0) {
			expr2set0(&e, lolepred);
		} else {
			expr2set0(&e, "true");
		}
		next << "/* lolepred */ ";
#else
		expr2set0(&e, "true");
#endif
		break;
	default:
		ASSERT(false);
		break;
	}
}

void
HyperCodegen::gen(GenCtx& ctx, Statement& e)
{
	switch (e.type) {
	case Statement::Type::Loop:
		code
			<< "while (1) { const auto __line_info = __LINE__;" << EOL;
		if (expr2var.find(e.expr.get()) == expr2var.end()) {
			gen(ctx, *e.expr);
		}

		code
			<< "  if (!(" << expr2get0(e.expr) << ")) {" << EOL
			<< "    LOG_GEN_TRACE(\"Loop check failed for '" << expr2get0(e.expr) << "' at %d\\n\", __line_info);" << EOL
			<< "    break;" << EOL
			<< "  }" << EOL
			<< "  LOG_GEN_TRACE(\"While start with '" << expr2get0(e.expr) << "' at %d\\n\", __line_info);" << EOL
			;
		for (auto& stmt : e.statements) {
			gen(ctx, *stmt);
		}
		code
			<< "  LOG_GEN_TRACE(\"While end at %d\\n\", __line_info);" << EOL
			<< "}; /* while */" << EOL;
		break;
	case Statement::Type::Assignment:
#if 0
		if (e.pred) {
			gen(ctx, *e.pred);
		}
#endif
		if (expr2var.find(e.expr.get()) == expr2var.end()) {
			gen(ctx, *e.expr);
		}

		{
			const std::string varname(e.var);
			bool new_variable;

			if (local_vars.top().find(varname) == local_vars.top().end()) {
				const auto id = unique_id();

				code.add_comments({"/* new local variable '", e.var, "' as '", id, "' */", EOL});
				local_vars.top()[varname] = {id, "", true};
				ASSERT(e.props.type.arity.size() == 1 && "must be scalar");
				new_decl(e.props.type.arity[0].type, id);

				new_variable = true;
			} else {
				code.add_comments({"/* updating variable '", e.var, "' as '", local_vars.top()[varname].data_ref, "' */", EOL});
				new_variable = false;
			}

			PredicateWrapper predicated(code);

			bool avoid_predicate = e.expr->is_select();
			if (e.pred && !avoid_predicate) {
				predicated.set_predicate(expr2get0(e.pred));
			}

			auto& data_ref = local_vars.top()[varname].data_ref;
			predicated << data_ref << " = "
				<< expr2get0(e.expr) << ";"
				<< " LOG_GEN_TRACE(\"set ("
				<< std::string(new_variable ? "new" : "update")
				<< ") variable '" << e.var << "' as '"
				<< local_vars.top()[varname].data_ref
				<< "' (as u64: %lu) at %d\\n\", (u64) " << data_ref << ", __LINE__);"
				<< EOL;
		}

		break;
	case Statement::Type::EffectExpr:
		gen(ctx, *e.expr);
		code << ";" << EOL;
		break;

	case Statement::Type::Emit:
		{
			if (e.pred) {
				gen(ctx, *e.pred);
			}
			if (expr2var.find(e.expr.get()) == expr2var.end()) {
				gen(ctx, *e.expr);
			}

			bool gen_pred = e.pred.get(); // && e.pred->type != Expression::Type::LolePred;

			code.flush();

			if (gen_pred) {
				code << "if (" << expr2get0(e.pred) << ") { /* EMIT|" << expr2get0(e.pred) << " */" << EOL; 
				lolepred = expr2get0(e.pred);
			} else {
				code << "{ /* EMIT */" << EOL;
			}

			lolearg = expr2var[e.expr.get()];
			Lolepop* nlol = move_to_next_op(ctx.p);
			if (nlol) {
				gen_lolepop(*nlol, ctx.p);
			} else {
				if (last_pipeline) {
					size_t i=0;
					code << "query.result.begin_line();" << EOL;
					for (auto& c : expr2var[e.expr.get()]) {
						code << "{" << EOL
							<< "size_t buffer_size = voila_cast<" << e.expr->props.type.arity[i].type << ">::good_buffer_size();" << EOL
							<< "char* str; char buffer[buffer_size];" << EOL
							<< "buffer_size = voila_cast<" << e.expr->props.type.arity[i].type << ">::to_cstr(str, &buffer[0], buffer_size, " << c << ");" << EOL
							<< "query.result.push_col(str, buffer_size);" << EOL
							<< "}" << EOL;
						i++;
					}
					code << "query.result.end_line();" << EOL;
				}
			}

			if (gen_pred) {
				code << "} /* EMIT|" << expr2get0(e.pred) << "*/" << EOL; 
			} else {
				code << "} /* EMIT */" << EOL;
			}
			break;
		}
	case Statement::Type::Done:
	case Statement::Type::MetaStmt:
		break;

	default:
		ASSERT(false);
		break;
	}
}

void
HyperCodegen::gen_lolepop(Lolepop& l, Pipeline& p)
{
	LOG_ERROR("lolpop %s\n", l.name.c_str());
	GenCtx ctx(l, p);
	for (auto& stmt : l.statements) {
		gen(ctx, *stmt);
	}
	code.flush();
}

void
HyperCodegen::gen_pipeline(Pipeline& p, size_t number)
{
	auto& ls = p.lolepops;

	lolepop_idx = 0;

	auto& l= *ls[lolepop_idx];

	lolepred = "";
	pipeline_end_gen = false;

	next
		<< "struct Pipeline_" << number << " : HyperPipeline {" << EOL
		<< " Pipeline_" << number << "(Query& q, size_t thread_id) : HyperPipeline(q, thread_id) " << EOL
		<< " {}" << EOL;

	std::string no_vec;

	if (!config.hyper_compiler_vectorize_pipeline) {
		no_vec = " NOVECTORIZE ";
	}
	next << no_vec << " void run() override {" << EOL;

	next << "ThisThreadLocal& thread = query.get_thread_local<ThisThreadLocal>(*this);" << EOL;

	// save 'next' before and after to inject the local variables
	const std::string prologue = clear_stream(next);
	wrap_lolepop(l, [&] () {
		gen_lolepop(l, p);
	});
	code.flush();
	const std::string epilogue = clear_stream(next);

	// re-init 'next', output local variables, add function
	next << prologue << clear_stream(local_decl) << epilogue;

	next << clear_stream(pipeline_end);

	// next << "  } /* while */" << EOL;
	next << " } /* run */" << EOL;
	next << "}; /*Pipeline_" << number << "*/" << EOL;

}