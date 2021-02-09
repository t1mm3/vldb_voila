#include "runtime.hpp"
#include "runtime_framework.hpp"

#include <sstream>
#include "utils.hpp"
#include "cg_fuji.hpp"
#include "cg_fuji_control.hpp"

#include "blend_context.hpp"

// #define ALWAYS_FLUSH_TO m_inflow_back_edge

#define CLITE_LOG(...) log_trace(__VA_ARGS__)

template<typename F, typename T>
void stack_for_each(const std::stack<T>& s, const F& fun)
{
	std::stack<T> tmp(s);

	while (!tmp.empty()) {
		fun(tmp.top());
		tmp.pop();
	}
}

static StmtPtr
find_blend(const StmtList& ss)
{
	for (auto& s : ss) {
		if (s->is_blend_op()) return s;

		StmtPtr r = find_blend(s->statements);
		if (r) return r;
	}

	return nullptr;
}

#define EOL std::endl

FujiCodegen::OpCtx::OpCtx(Pipeline& p, Lolepop& l)
 : pipeline(p), lolepop(l)
{
}

#include "voila.hpp"

#if 0
#define trace1_printf(...) printf(__VA_ARGS__);
#else
#define trace1_printf(...) 
#endif

FujiCodegen::FujiCodegen(QueryConfig& config)
 : Codegen(config)
{
}


FujiCodegen::~FujiCodegen()
{
	for (auto& b : m_blend_context) {
		delete b;
	}
}

std::string
FujiCodegen::get_lolepop_id(Lolepop& l, const std::string& n)
{
	return "op_" + l.name + "_" + n + std::to_string(++loleid_counter);
}

std::string
FujiCodegen::get_lolepop_id(const std::string& n)
{
	return get_lolepop_id(m_current_op->lolepop, n);
}

clite::Block*
FujiCodegen::get_current_state() const {
	ASSERT(m_current_blend->current_state);
	return m_current_blend->current_state;
}

DataGenExprPtr
FujiCodegen::get_current_var(const std::string& s)
{
	auto it = m_vars.find(s);
	if (it == m_vars.end()) {
		LOG_DEBUG("get_current_var: Cannot find '%s'\n", s.c_str());
		return nullptr;
	}

	auto& stack = it->second;
	if (stack.empty()) {
		LOG_DEBUG("get_current_var: Stack empty for '%s'\n", s.c_str());
		return nullptr;
	}

	auto r = stack.top();

	LOG_DEBUG("get_current_var: Resolve '%s' -> %p\n",
		s.c_str(), r.get());
	return r;
}

void
FujiCodegen::new_variable(const std::string& s, const DataGenExprPtr& e)
{
	ASSERT(m_vars.find(s) == m_vars.end() && "overwriting is not allowed");

	std::stack<DataGenExprPtr> vstack;
	vstack.push(e);
	m_vars.insert({s, std::move(vstack)});
}

DataGenExprPtr FujiCodegen::get_lolepop_arg(const size_t idx)
{
	ASSERT((int)idx < (int)m_op_tuple.size());
	auto& stack = m_op_tuple[idx];
	ASSERT(!stack.empty());
	return stack.top();
}
size_t FujiCodegen::get_lolepop_num_args() const
{
	return m_op_tuple.size();
}

DataGenExprPtr
FujiCodegen::gen(ExprPtr& e, bool pred)
{
	DataGenExprPtr ptr;

	trace1_printf("generating %s '%s'\n",
		e->type2string().c_str(), e->fun.c_str());
	ptr = _gen(e, pred);
	trace1_printf("generating %s '%s'  ---> %p\n",
		e->type2string().c_str(), e->fun.c_str(), ptr.get());

	if (ptr) {
		BlendContext* old_blend = expr_blends[e];
		BlendContext* new_blend = m_current_blend;

		// need to transition blend?
		if (old_blend && new_blend) {
			trace1_printf("Reading other blend's expression\n");
			trace1_printf("old_blend %p new_blend %p: data_expr %p\n", 
				old_blend, new_blend, ptr.get());
		}
	}
	return ptr;
}

void
FujiCodegen::put(const ExprPtr& e, const DataGenExprPtr& expr)
{
	ASSERT(e);
	ASSERT(expr);

	ASSERT(!get_ptr(e));	
	exprs[e] = expr;
	expr_blends[e] = m_current_blend;	
}

DataGenExprPtr
FujiCodegen::get_ptr(const ExprPtr& e)
{
	if (e && e->type == Expression::Function &&
			!e->fun.compare("tget")) {
		auto idx = TupleGet::get_idx(*e);
		return get_lolepop_arg(idx);
	}

	if (e && e->type == Expression::LolePred) {
		LOG_TRACE("LOLEPRED\n");
		stack_for_each(m_lolepred, [&] (auto& x) {
			LOG_TRACE("lolepred_stack: %s\n", x->src_data_gen.c_str());
		});
		LOG_TRACE("\n\n");
		ASSERT(!m_lolepred.empty() && m_lolepred.top());
		return m_lolepred.top();
		/*
		trace1_printf("put(lolepred=%p, e=%p): %p\n",
			m_lolepred.top().get(), e.get(), get_ptr(e).get());
		*/
	}

	if (e && e->type == Expression::Reference) {
		auto ptr = get_current_var(e->fun);
		//  ASSERT(ptr);

		clite::Builder builder(*get_current_state());
		builder << builder.comment("Access '" + e->fun + "'");

		LOG_TRACE("get_ptr(%p): Reference '%s' -> %p\n",
			e.get(), e->fun.c_str(), ptr.get());

		return ptr;
	}

	auto it = exprs.find(e);

	if (it == exprs.end()) {
		LOG_DEBUG("get_ptr(%p): not found\n", e.get());
		return nullptr;
	} else {
		return it->second;
	}
}

DataGenExprPtr
FujiCodegen::_gen(ExprPtr& e, bool pred)
{
	auto ptr = get_ptr(e);
	if (ptr) {
		return ptr;
	}

	auto m_data_gen = m_current_blend->data_gen.get();

	// we directly evaluate this, as it can only access 'm_op_tuple'
	if (e->type == Expression::Function &&
			!e->fun.compare("tget")) {
		// get_ptr will decode 'tget' and do the magic
		return get_ptr(e);
	}

	// generate children
	if (e->pred) {
		gen_pred(e->pred);
	}
	
	// materialize all variables
	if (e->props.gen_recurse.size() > 0) {
		for (size_t c=0; c<e->args.size(); c++) {
			auto& child = e->args[c];
			if (e->props.gen_recurse[c]) {
				gen_expr(child);
			}
		}
	} else {
		for (auto& child : e->args) {
			gen_expr(child);
		}
	}

	trace1_printf("FujiCodegen::gen Expression %p: %s %s\n",
		e.get(), e->type2string().c_str(), e->fun.c_str());

	ASSERT(e->fun.compare("tappend"));

	if (e->type == Expression::Reference) {
		return get_ptr(e);
	}

	if (e->type == Expression::Type::LolePred) {
		return get_ptr(e);
	}

	clite::Variable::Scope dest_scope = clite::Variable::Scope::Local;
	DataGenExprPtr result;

	auto prefetch_handler = [&] (auto type) {
		// extra handling for PREFETCH
		if (m_current_blend->config.prefetch > 0 &&
				(!e->props.first_touch_random_access.empty() || e->fun == "scan") &&
				m_data_gen->can_prefetch(e) == type) {

			// cannot allow this when we disable concurrent FSMs specifically
			if (!concurrent_fsms_disabled()) {
				clite::Builder prev(*get_current_state());
				prev << prev.inline_target();
				prev << prev.comment("BEFORE prefetch for '" + e->fun + "'");

				m_data_gen->prefetch(e, m_current_blend->config.prefetch-1);

				auto cached = m_flow_gen->fragment.new_block(get_lolepop_id("prefetch_" + e->fun));
				prev << prev.uncond_branch(cached, clite::BranchLikeliness::Always,
					clite::BranchThreading::MustYield);

				clite::Builder next(*cached);
				next << next.comment("AFTER prefetch for '" + e->fun + "'");

				m_current_blend->add_state(cached.get());
			}
		}
	};

	prefetch_handler(DataGen::PrefetchType::PrefetchBefore);

	bool match = false;

	// catch generic operations
	if (!match && e->is_get_morsel()) {
		const auto& ref = e->args[0]->fun;
		const std::string context_name("_" + ref + "_ctx");
		const auto id = "morsel";

		dest_scope = clite::Variable::Scope::ThreadWide;

		bool created = false;
		auto morsel_var = m_data_gen->new_simple_expr(id, "Morsel", dest_scope, false, "morsel:" + e->fun);
		// auto morsel_var = m_flow_gen->fragment.try_new_var(created, id, "Morsel", dest_scope);
		auto morsel_ctx = m_flow_gen->fragment.try_new_var(created, context_name, "MorselContext", dest_scope);


		clite::Builder builder(*get_current_state());
		if (created) {
			morsel_ctx->ctor_init = builder.literal_from_str("*this");
		}


		clite::StmtList statements;
		statements.push_back(builder.effect(
			builder.function(uppercase("get_" + e->fun), 
				builder.literal_from_str("thread." + ref),
				builder.reference(morsel_var->var),
				builder.reference(morsel_ctx))
		));


		if (m_flow_gen->is_parallel()) {
			builder << builder.predicated(builder.function("!", 
				builder.function(uppercase("can_get_pos_" + e->fun), builder.reference(morsel_var->var))
			), statements);
		} else {
			builder << statements;
		}

		m_data_gen->gen_extra(statements, e);
		put(e, morsel_var);

		match = true;
		result = get_ptr(e);
	}

	auto access_table = [] (const auto& tbl) {
			return "thread." + tbl;
		};

	if (!match && !e->fun.compare("bucket_build")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		std::ostringstream impl;
		impl
			<< "if (!schedule_idx) {" << EOL
			<< access_table(tbl) << "->create_buckets(this);" << EOL
			<< "}" << EOL;

		clite::Builder builder(*get_current_state());
		builder << builder.plain(impl.str());

		match = true;
		result = nullptr;
		// put(e, m_data_gen->new_simple_expr(nullptr, false));
		//return get_ptr(e);
	}

	if (!match && !e->fun.compare("bucket_flush")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		std::ostringstream impl;
		impl
			<< "if (!schedule_idx) {" << EOL
			<< access_table(tbl) << "->flush2partitions();" << EOL
			<< "}" << EOL;

		clite::Builder builder(*get_current_state());
		builder << builder.plain(impl.str());

		match = true;
		result = nullptr;
		// put(e, m_data_gen->new_simple_expr(nullptr, false));
		//return get_ptr(e);
	}

#if 1
	if (!match && !e->fun.compare("write_pos")) {
		const auto& ref = e->args[0]->fun;

		bool created;

		dest_scope = clite::Variable::Scope::ThreadWide;

		auto pos_var = m_data_gen->new_simple_expr("", "Position", dest_scope, false, "write_pos");
#if 0
		auto pos_var = m_flow_gen->fragment.new_var(unique_id(), "Position",
			dest_scope);
#endif
		auto view_var = m_flow_gen->fragment.try_new_var(created, unique_id(),
			"ITable::ThreadView", dest_scope);
		clite::Builder builder(*get_current_state());
		if (created) {
			view_var->ctor_init = builder.literal_from_str("*thread." + ref + ", *this");
		}

		clite::Factory factory;
		clite::StmtList statements;
		statements.emplace_back(factory.assign(pos_var->var,
			factory.function(uppercase("get_write_pos"),
				factory.reference(view_var),
				get_ptr(e->pred)->get_len())));


		put(e, pos_var);

		builder << statements;
		m_data_gen->gen_extra(statements, e);

		match = true;
		result = get_ptr(e);
	}
#endif
	
	if (!match && e->is_get_pos()) {
		auto& child = e->args[0];
		auto child_ptr = get_ptr(child);
		ASSERT(child_ptr);

		auto dest_var = m_data_gen->new_simple_expr("", "Position", dest_scope, false, "pos:" + e->fun);

		clite::Builder builder(*get_current_state());
		clite::StmtList statements;

		statements.push_back(builder.assign(dest_var->var, builder.function(uppercase("get_" + e->fun), {
			builder.reference(child_ptr->var), builder.literal_from_int(m_data_gen->get_unroll_factor()),
		})));

		put(e, dest_var);

		if (!e->fun.compare("scan_pos")) {
			statements.push_back(builder.CLITE_LOG("scan_pos: file %s", {
				builder.literal_from_str("__FILE__")
			}));
		}

		builder << statements;
		m_data_gen->gen_extra(statements, e);

		match = true;
		result = get_ptr(e);
	}

	if (!match && !e->fun.compare("selvalid")) {
		auto& child = e->args[0];
		auto child_ptr = get_ptr(child);
		ASSERT(child_ptr);

		auto dest_var = m_data_gen->new_simple_expr("", "pred_t", dest_scope, true, "selvalid");

		clite::Builder builder(*get_current_state());
		clite::StmtList statements;


		statements.push_back(builder.assign(dest_var->var, builder.function("IS_VALID",
			builder.reference(child_ptr->var)
		)));

		put(e, dest_var);

		builder << statements;
		m_data_gen->gen_extra(statements, e);

		match = true;
		result = get_ptr(e);
	}

	bool is_global_aggr = false;
	e->is_aggr(&is_global_aggr);
	if (!match && is_global_aggr) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		clite::Builder builder(*get_current_state());

		if (!global_agg_bucket) {
			clite::Factory f;
			global_agg_bucket = f.literal_from_str("m_global_aggr_bucket");

			std::ostringstream s;

			s	<< at_begin << EOL
				<< "if (!m_global_aggr_bucket) {" << EOL
				<< "  auto table = " << "thread." << tbl << ";" << EOL
				<< "  Block* block = table->hash_append_prealloc<false>(1);" << EOL
				<< "  char* new_bucket = block->data + (block->width * block->num);" << EOL
				<< "  void** buckets = table->get_hash_index();" << EOL
				<< "  buckets[0] = new_bucket;" << EOL
				<< "  table->hash_append_prune<false>(block, 1);" << EOL
				<< "  m_global_aggr_bucket = new_bucket; " << EOL
				// << "  printf(\"" << e.fun << ": index=%lu count=%lu bucket=%lu mask=%lu\\n\", index, table->count, table->buckets[index], table->bucket_mask);" << EOL
				<< "  LOG_TRACE(\"Bucket %p\\n\", m_global_aggr_bucket);" << EOL
				<< "}" << EOL
				;

			at_begin = s.str();
		}

		clite::StmtList statements;
		m_data_gen->gen_extra(statements, e);
		builder << statements;

		auto dummy = m_data_gen->new_simple_expr(unique_id(),
			"bool", clite::Variable::Scope::Local, true, "dummy");
		put(e, dummy);

		match = true;
		result = get_ptr(e);
	}

	if (!match) {
		// the remainder is handled by DataGen
		if (pred) {
			result = m_data_gen->gen_get_pred(e);
		} else {
			result = m_data_gen->gen_get_expr(e);
		}		
	}
	return result;
}


struct SavedExpr {
	std::string var_name;

	DataGenExprPtr expr_data;

	DataGenBufferColPtr buffer;

	bool predicate;
};

struct SavedLolearg {
	size_t index;

	DataGenExprPtr expr_data;
	DataGenBufferColPtr buffer;
};

void
FujiCodegen::gen_flush_backedge(const DataGenBufferPtr& buffer, DataGen& gen,
	clite::Block* followup, clite::Block* flush_state, clite::Block* inflow_back_edge,
	const std::string& dbg_name)
{
	clite::BranchLikeliness inflow_likeliness = clite::BranchLikeliness::Likely;

	{
		clite::Builder builder(followup);
		builder
			<< builder.CLITE_LOG("gen_flush_backedge " + dbg_name, {});		
	}

	if (flush_state) {
		gen.on_buffer_non_empty(buffer, followup,
			flush_state, dbg_name);
		inflow_likeliness = clite::BranchLikeliness::Unlikely;
	}

	{
		clite::Builder builder(followup);

		builder
			<< builder.CLITE_LOG("No flush backedge " + dbg_name, {})
			<< builder.uncond_branch(inflow_back_edge, inflow_likeliness);
	}
}

void
FujiCodegen::gen_flush_backedge(const FujiCodegen::LoopBackedge& lbe)
{
	ASSERT(lbe.first_buffer_write_followup);
	ASSERT(lbe.last_buffer);
	ASSERT(lbe.last_buffer_flush);
	ASSERT(lbe.data_gen);
	gen_flush_backedge(lbe.last_buffer, *lbe.data_gen, lbe.first_buffer_write_followup,
		lbe.last_buffer_flush, lbe.inflow_back_edge, "loop_wrap_edge");
}	

#include <regex>

void
FujiCodegen::gen_blend(BlendStmt& blend_op, const BlendConfig* new_blend_config)
{
	LOG_DEBUG("\n\nBLEND ENTER\n\n");
	auto blend_predicate = blend_op.get_predicate();

	{
		clite::Builder builder(get_current_state());

		builder << builder.comment("enter blend");
	}

	ASSERT(blend_predicate);
	// ASSERT(blend_predicate->type == Expression::LolePred);
	auto old_pred = m_lolepred.top();

	auto filter_captured_vars = [&] (const auto& vars, const std::string& dbg_class) {
		LOG_ERROR("dbg_class: %s\n", dbg_class.c_str());
		std::unordered_set<std::string> result;

		static std::unordered_set<std::string> skip_types = {"Morsel", "Position"};

		for (auto& var : vars) {
			result.insert(var);


			auto ptr = get_current_var(var);
			if (!ptr) {
				// ASSERT(false);
				LOG_ERROR("var: %s skip\n", var.c_str());
				continue;
			}
			LOG_ERROR("var: %s, type %s\n", var.c_str(), ptr->var->type.c_str());

			if (skip_types.find(ptr->var->type) != skip_types.end()) {
				result.erase(var);
				LOG_ERROR("delete var\n");
			}

			// TODO: need to mark such variables early
			if (var.find("_valid_pos")!=std::string::npos ||
					var.find("_valid_morsel")!=std::string::npos) {
				result.erase(var);
				LOG_ERROR("delete var\n");	
			}
		}

		return result;
	};

	const auto captured_input_variables = filter_captured_vars(blend_op.crossing_variables.all_inputs,
		"input");
	const auto captured_output_variables = filter_captured_vars(blend_op.crossing_variables.all_outputs,
		"output");

	std::ostringstream dbg_comment;

	auto old_blend = m_current_blend;
	BlendContext* new_blend = create_blend_context(new_blend_config, old_blend);

	ASSERT(m_current_blend->config.concurrent_fsms == new_blend->config.concurrent_fsms
		&& "todo");

	std::vector<SavedExpr> saved_in_vars;
	std::vector<SavedLolearg> saved_in_largs;
	SavedLolearg saved_in_lolepred;

	clite::BlockPtr in_blend_flush = m_flow_gen->fragment.new_block(
		get_lolepop_id(m_current_op->lolepop, "entry"));
	clite::BlockPtr post_blend_flush = m_flow_gen->fragment.new_block(
		get_lolepop_id(m_current_op->lolepop, "entry"));

	DataGenBufferPtr input_buffer = std::make_shared<DataGenBuffer>(config,
		old_blend->config, new_blend->config, unique_id(), "input");
	DataGenBufferPosPtr input_buffer_write_pos(
		old_blend->data_gen->write_buffer_get_pos(input_buffer,
#ifndef ALWAYS_FLUSH_TO
			in_blend_flush.get(),
#else
			ALWAYS_FLUSH_TO,
#endif
			nullptr, old_pred, ""));

	auto write_buffer_followup = [&] (bool input,
			const DataGenBufferPtr& buffer, auto& data_gen) {
		const std::string dbg_name(input ? "input" : "output");
		std::ostringstream impl;
		clite::Block* curr_state = get_current_state();

		// FIXME: decides which back-edge to take
		// right now, we assume that we will preferably drain
		// preceding buffers.
		//
		// Alternatively, one can always go back to the data inflow
		// and, consequently, enforce push-driven buffer flushing.
		// See #15

		clite::BlockPtr followup = m_flow_gen->fragment.new_block(
			get_lolepop_id(m_current_op->lolepop, "followup"));

		clite::Builder builder(curr_state);

		builder << builder.CLITE_LOG("write_buffer_followup: followup", {})
			<< builder.uncond_branch(followup, clite::BranchLikeliness::Always);

		// Drain preceding buffer, if non-empty
		if (m_flush_back_edge) {
			gen_flush_backedge(m_flush_buffer, data_gen, followup.get(), m_flush_back_edge,
				m_inflow_back_edge, "backedge_drain");
		} else {
			// delay that. to allow flushing the last buffer in the loop
			// the problem is that we compile that buffer (the last) later,
			// hence we delay that
			if (!m_loop_backedges.empty()) {
				auto& lbe = m_loop_backedges.top();
				if (!lbe.first_buffer_write_followup) {
					lbe.first_buffer_write_followup = followup.get();
					lbe.inflow_back_edge = m_inflow_back_edge;
					lbe.data_gen = std::addressof(data_gen);
				}
			}
		}
	};

	auto read_buffer_on_empty = [&] (bool input) {
		// schedule better buffer

		return m_inflow_back_edge;
	};


	// chain
	ASSERT(!old_blend->output_buffer);
	old_blend->output_buffer = input_buffer;
	ASSERT(!new_blend->input_buffer);
	new_blend->input_buffer = input_buffer;


	auto debug_print_var = [&] (const std::string& cat, const auto& cap_var) {
#if 0
		auto ptr = get_current_var(cap_var);

		std::string cid = "<NO_ID>";

		if (ptr) {
			if (ptr) {
				cid = ptr->get_c_id();	
			}
		}

		std::ostringstream s;
		s 	<< "/* " + cat + ": '" + cap_var + "' "
			<< " as '" << cid << "'"
			<< "*/\n";

		dbg_comment << s.str();
		get_current_state()->append_plain(s.str(), "", {});
#endif
	};

	// buffers variables
	auto write_to_buffer = [&] (const std::string& dbg_name, bool is_input,
			const auto& captured_variables,
			BlendContext& blend, const DataGenBufferPosPtr& write_pos,
			std::vector<SavedExpr>& saved_variables,
			std::vector<SavedLolearg>& saved_lole_args,
			SavedLolearg& saved_lolepred) {
		auto& dgen = *blend.data_gen;

		for (const auto& cap_var : captured_variables) {
			debug_print_var(dbg_name, cap_var);
		}


		for (size_t i=0; i<get_lolepop_num_args(); i++) {
			auto ptr = get_lolepop_arg(i);
			ASSERT(ptr);

			auto buf = dgen.write_buffer_write_col(write_pos, ptr,
				"lolearg_" + std::to_string(i));
			saved_lole_args.push_back({i, ptr, buf});
		}

		{
			auto ptr = m_lolepred.top();
			ASSERT(ptr);

			auto buf = dgen.write_buffer_write_col(write_pos, ptr,
					"lolepred");
			saved_lolepred = {0, ptr, buf};
		}

		// materialize 'inputs'
		auto write_var = [&] (const auto& cap_var, bool pred) {
			
			auto ptr = get_current_var(cap_var);
			ASSERT(ptr);

			if (ptr->predicate != pred) {
				return;
			}

			trace1_printf("%s = %s\n", dbg_name.c_str(), cap_var.c_str());
			auto buf = dgen.write_buffer_write_col(write_pos, ptr, cap_var);

			// save old expression
			saved_variables.push_back(SavedExpr {cap_var, ptr, buf, ptr->predicate});
		};

		for (const auto& cap_var : captured_variables) {
			write_var(cap_var, false);
		}
		for (const auto& cap_var : captured_variables) {
			write_var(cap_var, true);
		}
		dgen.write_buffer_commit(write_pos);
	};

	auto stack_magic = [&] (const std::string& stack_name, bool is_input,
			auto& st, const DataGenExprPtr& reader,
			const DataGenBufferPosPtr& read_pos, auto& dgen,
			const std::string& var_name, const std::string& dbg_class) {

		LOG_DEBUG("stack_name %s, is_input %d\n",
			stack_name.c_str(), is_input);

		stack_for_each(st, [&] (const auto& item) {
			LOG_DEBUG("item source=%s\n", item->src_data_gen.c_str());
		});


		if (is_input) {
#if 0
			dbg << "/* stack_magic(input '" << stack_name << "' reader='"
				<< reader->get_c_id() << "'";
#endif
			st.push(reader);
		} else {
#if 0
			dbg << "/* stack_magic(output '" << stack_name << "' stack_size=" << st.size();
#endif
			// remove from stack, or assign to new variable
			ASSERT(!st.empty());
			st.pop();

			if (st.empty()) {
				// must be a new variable created from within BLEND
				ASSERT(!var_name.empty());

				LOG_ERROR("new('%s'): reader source=%s\n",
					var_name.c_str(), reader->src_data_gen.c_str());

				st.push(reader);

				// output buffer, we might need to replace the mask
				dgen.buffer_overwrite_mask(reader, read_pos);
			} else {
				auto& new_expr = st.top();

				LOG_ERROR("copy: reader source=%s -> new_expr source=%s\n",
					reader->src_data_gen.c_str(), new_expr->src_data_gen.c_str());

				ASSERT(reader->src_data_gen == new_expr->src_data_gen);
#if 0
				dbg << " copy from reader '" << reader->get_c_id() << "' into '"
					<< new_expr->get_c_id() << "'";
#endif
				dgen.copy_expr(new_expr, reader);

				// output buffer, we might need to replace the mask
				dgen.buffer_overwrite_mask(new_expr, read_pos);
			}
		}
#if 0
		dbg << "*/" << EOL;
		get_current_state()->append_plain(dbg.str());
#endif
	};
	// load variables from buffer
	auto load_from_buffer = [&] (const std::string& dbg_name, bool is_input,
			BlendContext& blend, const DataGenBufferPosPtr& read_pos,
			const std::vector<SavedExpr>& saved_variables,
			const std::vector<SavedLolearg>& saved_lole_args,
			const SavedLolearg& saved_lolepred) {
		LOG_DEBUG("load from buffer: %s\n", dbg_name.c_str());
		auto& dgen = *blend.data_gen;

#ifdef TRACE
		clite::Builder builder(get_current_state());
		builder << builder.plain("g_buffer_name = \"" +dbg_name + "/" + read_pos->buffer->c_name + "\";");
		builder << builder.plain("LOG_ERROR(\"%s: %s: New buffer\\n\", g_pipeline_name, g_buffer_name);");
#endif


		for (auto& larg : saved_lole_args) {
			auto reader = dgen.read_buffer_read_col(read_pos,
				larg.buffer, "lolearg_" + std::to_string(larg.index));

			auto dst = get_lolepop_arg(larg.index);
			ASSERT(dst);

			auto& st = m_op_tuple[larg.index];
			stack_magic(dbg_name + ":arg", is_input, st, reader, read_pos, dgen, "", "larg");
		}

		{
			auto reader = dgen.read_buffer_read_col(read_pos,
				saved_lolepred.buffer, "lolepred");

			auto& st = m_lolepred;
			stack_magic(dbg_name + ":lpred", is_input, st, reader, read_pos, dgen, "", "lpred");
		}

		// overwrite Expression cache

		auto read_vars = [&] (auto& expr, bool pred) {
			if (expr.predicate != pred) {
				return;
			}

			auto reader = dgen.read_buffer_read_col(read_pos,
				expr.buffer, expr.var_name);

			auto dst = get_current_var(expr.var_name);
			ASSERT(dst && "Disallow overwriting for now, but should work");

			auto& st = m_vars[expr.var_name];

			ASSERT(!expr.var_name.empty());
			stack_magic(dbg_name + ":var", is_input, st, reader, read_pos, dgen, expr.var_name, "var");
		};

		for (const auto& expr : saved_variables) {
			read_vars(expr, false);
		}

		for (const auto& expr : saved_variables) {
			read_vars(expr, true);
		}
		dgen.read_buffer_commit(read_pos);
	};


	write_to_buffer("input", true, captured_input_variables,
		*old_blend, input_buffer_write_pos, saved_in_vars, saved_in_largs,
		saved_in_lolepred);
	write_buffer_followup(true, input_buffer, *old_blend->data_gen);

	LOG_DEBUG("TODO: Pre-Blend Buffering boundary\n");
	{
		clite::Builder builder(get_current_state());

		builder << builder.comment("Pre-Blend Buffering boundary");
	}

	// === start new fragment ===

	m_current_blend = new_blend;
	new_blend->add_state(in_blend_flush.get());
	m_buffers.push_back({input_buffer, in_blend_flush.get(),
		&new_blend->config});

	DataGenBufferPosPtr input_buffer_read_pos(
		new_blend->data_gen->read_buffer_get_pos(input_buffer, nullptr,
			read_buffer_on_empty(true), "inflow_back_edge")
		);

	load_from_buffer("input", true, *new_blend, input_buffer_read_pos,
		saved_in_vars, saved_in_largs, saved_in_lolepred);

	m_flush_back_edge = in_blend_flush.get();
	m_flush_buffer = input_buffer;

	LOG_DEBUG("\n\nBLEND PRE STATEMENTS\n\n");
	{
		clite::Builder builder(get_current_state());

		builder << builder.comment("BLEND PRE STATEMENTS");
	}
	// generate statements
	for (auto& stmt : blend_op.statements) {
		gen(stmt);
	}

	LOG_DEBUG("\n\nBLEND POST STATEMENTS\n\n");
	{
		clite::Builder builder(get_current_state());

		builder << builder.comment("BLEND POST STATEMENTS");
	}

	ASSERT(blend_predicate);
	// ASSERT(blend_predicate->type == Expression::LolePred);
	auto new_pred = m_lolepred.top();


	std::vector<SavedExpr> saved_out_vars;
	std::vector<SavedLolearg> saved_out_largs;
	SavedLolearg saved_out_lolepred;

	auto copy_old_blend = create_blend_context(&old_blend->config, new_blend);
	DataGenBufferPtr output_buffer = std::make_shared<DataGenBuffer>(config,
		new_blend->config, old_blend->config, unique_id(), "output");
	DataGenBufferPosPtr output_buffer_write_pos(
		new_blend->data_gen->write_buffer_get_pos(output_buffer,
#ifndef ALWAYS_FLUSH_TO
			post_blend_flush.get(),
#else
			ALWAYS_FLUSH_TO,
#endif
			nullptr, new_pred, "post_blend"));

	// get outputs
	// ASSERT(captured_output_variables.size() > 0);
	write_to_buffer("output", false, captured_output_variables,
		*new_blend, output_buffer_write_pos, saved_out_vars, saved_out_largs,
		saved_out_lolepred);
	write_buffer_followup(false, output_buffer, *new_blend->data_gen);

	LOG_DEBUG("TODO: Post-Blend Buffering boundary\n");

	copy_old_blend->add_state(post_blend_flush.get());
	m_current_blend = copy_old_blend;
	m_buffers.push_back({output_buffer, post_blend_flush.get(),
		&copy_old_blend->config});

	// === start new fragment ===
	DataGenBufferPosPtr output_buffer_read_pos(
		copy_old_blend->data_gen->read_buffer_get_pos(
			output_buffer, nullptr, read_buffer_on_empty(false), "inflow_back_edge"));


	load_from_buffer("output", false, *copy_old_blend, output_buffer_read_pos,
		saved_out_vars, saved_out_largs, saved_out_lolepred);



	m_flush_back_edge = post_blend_flush.get();
	m_flush_buffer = output_buffer;

	if (!m_loop_backedges.empty()) {
		auto& lbe = m_loop_backedges.top();

		lbe.last_buffer_flush = post_blend_flush.get();
		lbe.last_buffer = output_buffer;
	}

	// chain them
#ifdef REMOVEME
	ASSERT(!new_blend->output_buffer);
	ASSERT(!copy_old_blend->input_buffer);
#endif
	new_blend->output_buffer = output_buffer;
	copy_old_blend->input_buffer = input_buffer;
#if 0
	get_current_state()->append_plain(dbg_comment.str());
#endif

	LOG_DEBUG("\n\nBLEND EXIT\n\n");
	{
		clite::Builder builder(get_current_state());

		builder << builder.comment("BLEND EXIT");
	}


#ifdef TRACE
	clite::Builder builder(get_current_state());
	builder << builder.plain("g_buffer_name = \"\";");
	builder << builder.plain("LOG_ERROR(\"%s: %s: New buffer\\n\", g_pipeline_name, g_buffer_name);");
#endif
}

bool only_prefetch_diff(const BlendConfig& o, const BlendConfig& n)
{
	return o.computation_type == n.computation_type &&
		o.concurrent_fsms == n.concurrent_fsms &&
		o.prefetch != n.prefetch;
}

void
FujiCodegen::gen(StmtPtr& e)
{
	trace1_printf("FujiCodegen::gen Statement %p(%s)\n", e.get(), e->type2string().c_str());

	auto m_data_gen = m_current_blend->data_gen.get();

	switch (e->type) {
	case Statement::Type::BlendStmt: {
			BlendStmt& blend_op(*(BlendStmt*)e.get());
			BlendConfig* config = nullptr;

			if (m_pipeline_space_point) {
				ASSERT(m_blends_done < m_pipeline_space_point->point_flavors.size());

				config = m_pipeline_space_point->point_flavors[m_blends_done];
				m_blends_done++;
			} else {
				config = blend_op.blend_op_get_config().get();
			}


			auto& old_config = m_current_blend->config;

			bool gblend = true;

			if (gblend && (!config || config->is_null())) {
				gblend = false;
			}

			if (gblend && only_prefetch_diff(old_config, *config)) {
				gblend = false;

				m_current_blend->config.prefetch = config->prefetch;
			}	

			if (gblend) {
				LOG_DEBUG("<BLEND CONFIG=%s>\n", config->to_string().c_str());
				gen_blend(blend_op, config);
				LOG_DEBUG("</BLEND>\n");
			} else {
				LOG_DEBUG("<NULL CONFIG=%s>\n", config ? config->to_string().c_str() : "NONE");
				// NULL blend, do nothing
				for (auto& stmt : e->statements) {
					gen(stmt);
				}
				m_current_blend->config.prefetch = old_config.prefetch;
				LOG_DEBUG("</NULL>\n");
			}

			break;
		}

	case Statement::Type::Loop: {
			m_loop_backedges.push(LoopBackedge());

			clite::Block* prev_state = m_current_blend->current_state;

			clite::BlockPtr loop_body = m_flow_gen->fragment.new_block(get_lolepop_id("loop_body"));

			clite::Builder prev(prev_state);
			prev << prev.uncond_branch(loop_body.get(), clite::BranchLikeliness::Always);

			m_current_blend->add_state(loop_body.get());
			
			clite::BlockPtr loop_exit = m_flow_gen->fragment.new_block(get_lolepop_id("loop_exit"));

			trace1_printf("loop(%p): pre m_current_blend %p\n", e.get(), m_current_blend);

			// compute predicate & possibly jump to 'loop_exit'
			{
				auto condition = gen_pred(e->expr);
				clite::Builder builder(m_current_blend->current_state);

				auto cond = m_current_blend->data_gen->is_predicate_non_zero(condition);

				builder
					<< builder.log_trace("loop check @ %d",
						{ builder.literal_from_str("__LINE__") })
					<< builder.cond_branch(builder.function("!", cond), loop_exit.get(),
						clite::BranchLikeliness::Unlikely)
					;
			}

			// generate loop body
			for (auto& stmt : e->statements) {
				gen(stmt);
			}

			trace1_printf("loop(%p): post m_current_blend %p\n", e.get(), m_current_blend);

			// jump back into 'loop_entry'
			clite::Builder builder(m_current_blend->current_state);
			builder << builder.uncond_branch(loop_body.get(), clite::BranchLikeliness::Likely);

			// goto loop end
			m_current_blend->add_state(loop_exit.get());

			// get 'm_flush_back_edge' and connect to first buffer
			{
				ASSERT(!m_loop_backedges.empty());
				auto& lbe = m_loop_backedges.top();

				if (lbe.first_buffer_write_followup) {
					gen_flush_backedge(lbe);
				}
			}

			m_loop_backedges.pop();


			break;
		}
	case Statement::Type::Assignment:
	case Statement::Type::EffectExpr:
		if (e->expr->pred) {
			gen_pred(e->expr->pred);
		}

		gen_expr(e->expr);

		if (e->type == Statement::Type::EffectExpr) {
			m_data_gen->gen_stmt(e);
		}

		if (e->type == Statement::Type::Assignment && e->expr->is_get_morsel()) {
			bool new_var = m_vars.find(e->var) == m_vars.end();
			auto src = get_ptr(e->expr);

			if (new_var) {
				new_variable(e->var, src);
			}

			break;
		}
		if (e->type == Statement::Type::Assignment) {
			bool new_var = m_vars.find(e->var) == m_vars.end();
			auto src = get_ptr(e->expr);

			ASSERT(src);
			if (new_var) {
				LOG_DEBUG("new variable '%s'\n", e->var.c_str());
				auto clone = m_data_gen->clone_expr(src);
				new_variable(e->var, std::move(clone));
			}

			auto dst = get_current_var(e->var);
			ASSERT(dst->var != src->var);

			m_data_gen->copy_expr(dst, src);
		}
		break;

	case Statement::Type::Emit:
		{
			if (e->expr->pred) {
				gen_pred(e->expr->pred);
			}

			if (e->expr->type == Expression::Type::Function &&
					!e->expr->fun.compare("tappend")) {


				// trigger all inputs
				for (auto& arg : e->expr->args) {
					gen_expr(arg);
				}

				std::vector<std::stack<DataGenExprPtr>> new_op_tuple;
				new_op_tuple.reserve(e->expr->args.size());
				for (auto& arg : e->expr->args) {
					std::stack<DataGenExprPtr> stack;
					stack.push(get_ptr(arg));
					new_op_tuple.push_back(std::move(stack));
				}

				m_lolepred.push(
					m_data_gen->gen_emit(e,
						get_next_op(m_current_op->pipeline)));
				trace1_printf("LOLEPRED pre-emit%p\n", m_lolepred.top().get());

				auto old_op_tuple = m_op_tuple;
				m_op_tuple = std::move(new_op_tuple);
				Lolepop* nlol = move_to_next_op(m_current_op->pipeline);
				if (nlol) {
					gen_lolepop(*nlol, m_current_op->pipeline);
				} else {
					if (last_pipeline) {
						m_data_gen->gen_output(e);
					}
				}

				m_op_tuple = old_op_tuple;
				m_lolepred.pop();
				ASSERT(!m_lolepred.empty());
				trace1_printf("LOLEPRED post-emit %p\n", m_lolepred.top().get());
			} else {
				ASSERT(false && "todo");
			}

			break;
		}
	case Statement::Type::Done: {
			clite::Builder builder(get_current_state());
			clite::Block* last_state = m_flow_gen->last_state();
			builder << builder.uncond_branch(last_state,
				clite::BranchLikeliness::Always);
			break;
		}

	case Statement::Type::MetaStmt:
		{
			MetaStmt* meta = (MetaStmt*)e.get();
			if (meta->meta_type == MetaStmt::MetaType::RefillInflow) {
				ASSERT(!m_inflow_back_edge_generated);
				clite::Block* prev_state = m_current_blend->current_state;

				clite::Builder prev(prev_state);
				prev << prev.uncond_branch(m_inflow_back_edge, clite::BranchLikeliness::Likely);
				m_current_blend->add_state(m_inflow_back_edge);

				m_inflow_back_edge_generated++;
			}

			if (m_flow_gen->is_parallel() &&
					meta->meta_type == MetaStmt::MetaStmt::FsmExclusive) {
				MetaFsmExclusive* fsm = (MetaFsmExclusive*)meta;

				std::ostringstream impl;
				if (fsm->begin) {
					//impl<< "ASSERT(!schedule_lock_count);" << EOL;
					impl<< "schedule_lock_count++;" << EOL;
					//impl<< "LOG_DEBUG(\"LWT %d@%d: FSM acquire %d\\n\", "
					//	"schedule_idx, __LINE__, schedule_lock_count);" << EOL;
				} else {
					impl<< "schedule_lock_count--;" << EOL;
					/*
					impl<< "LOG_DEBUG(\"LWT %d@%d: FSM release %d\\n\", "
						"schedule_idx, __LINE__, schedule_lock_count);" << EOL;
					impl<< "ASSERT(!schedule_lock_count);" << EOL;
					*/
				}

				clite::Builder builder(get_current_state());
				builder << builder.plain(impl.str());
			}
			break;
		}
	default:
		ASSERT(false);
		break;
	}
}

void
FujiCodegen::gen_lolepop(Lolepop& l, Pipeline& p)
{
	LOG_DEBUG("<OP %s>\n", l.name.c_str());
	clite::BlockPtr op_state = m_flow_gen->fragment.new_block(get_lolepop_id(l, "entry"));
	clite::BlockPtr op_exit = m_flow_gen->fragment.new_block(get_lolepop_id(l, "exit"));

	if (!m_inflow_back_edge) {
		m_inflow_back_edge = m_flow_gen->fragment.new_block("inflow").get();
	}

	OpCtx* old_op_ctx = m_current_op;

	if (old_op_ctx) {
		auto& next_pred = m_lolepred.top();
		clite::Builder builder(get_current_state());

		clite::ExprPtr next_cond = next_pred ? 
			m_current_blend->data_gen->is_predicate_non_zero(next_pred) : 
			builder.literal_from_str("true");

		builder
			<< builder.cond_branch(next_cond, op_state, clite::BranchLikeliness::Always)
			<< builder.uncond_branch(op_exit, clite::BranchLikeliness::Always)
			;
	}

	{
		clite::Builder builder(*op_state);

		builder << builder.inline_target();
		builder << builder.log_trace("enter op '" + l.name + "'@ %d",
			{ builder.literal_from_str("__LINE__") });
	}

	m_current_blend->add_state(op_state.get());

	OpCtx this_ctx(p, l);
	m_current_op = &this_ctx;

	if (!lolepop_count) {
		m_lolepred.push(m_current_blend->data_gen->new_non_selective_predicate());
		trace1_printf("LOLEPRED pipeline %p\n", m_lolepred.top().get());		
	}

	lolepop_count++;

	for (auto& stmt : l.statements) {
		gen(stmt);
	}

	// restore context and go to new state
	clite::Builder builder(m_current_blend->current_state);

	builder << builder.uncond_branch(op_exit.get(),
		clite::BranchLikeliness::Always);

	m_current_op = old_op_ctx;
	m_current_blend->add_state(op_exit.get());

	{
		clite::Builder builder(*op_exit);
		builder << builder.inline_target();
		builder << builder.log_trace("leave op '" + l.name + "'@ %d",
			{ builder.literal_from_str("__LINE__") });
	}

	LOG_DEBUG("</OP> %s\n", l.name.c_str());

}

void
FujiCodegen::gen_pipeline(Pipeline& p, size_t number)
{
	m_buffers.clear();
	m_vars.clear();
	m_op_tuple.clear();
	exprs.clear();
	m_reset_vars.clear();
#ifdef TRACE
	key_checks.clear();
#endif
	at_fin.clear();
	at_begin = "";
	global_agg_bucket = nullptr;

	m_blends_done = 0;

	BlendConfig blend_config(config.default_blend);

	{	
		m_pipeline_space_point = nullptr;

		if (config.full_blend) {
			if (!config.full_blend->is_valid()) {
				std::cerr << "Invalid full blend" << std::endl;
				ASSERT(false);
				exit(1);
			}
			auto& point = *config.full_blend;
			ASSERT(number < point.pipelines.size());

			m_pipeline_space_point = &point.pipelines[number];

			blend_config = *m_pipeline_space_point->flavor;
		} else {
			auto per_pipeline_flavor = config.pipeline_default_blend.find(number);
			if (per_pipeline_flavor != config.pipeline_default_blend.end()) {
				blend_config = per_pipeline_flavor->second;
			}
		}
	}

	m_flow_gen = std::make_unique<FlowGenerator>(*this, blend_config.concurrent_fsms);
	m_current_blend = create_blend_context(&blend_config, nullptr);

	m_flow_gen->fragment.dbg_name = "pipeline" + std::to_string(number);

	auto m_data_gen = m_current_blend->data_gen.get();

	printf("Generating pipeline %d\n", number);
	auto& ls = p.lolepops;

	lolepop_idx = 0;
	lolepop_count = 0;

	auto& l= *ls[lolepop_idx];

	m_flow_gen->clear();

	m_flush_back_edge = nullptr;
	m_flush_buffer = nullptr;

	m_inflow_back_edge = nullptr;
	m_inflow_back_edge_generated = 0;

	std::ostringstream impl;
	std::ostringstream decl;

	std::ostringstream structs;
	std::ostringstream regist;



	impl<< "#include <immintrin.h>" << EOL
		<< "struct Pipeline_" << number << " : FujiPipeline {" << EOL
		<< " Pipeline_" << number << "(Query& q, size_t thread_id) : FujiPipeline(q, thread_id, \"Pipeline_" << number << "\") " << EOL
		<< " {" << EOL;

	m_data_gen->begin_eval_scope(true);

	// Generate code
	wrap_lolepop(l, [&] () {
		gen_lolepop(l, p);
	});

	gen_global_structs(structs, regist, Record::Scope::Pipeline);

	impl<< " }" << EOL
		<< "/* <Per pipeline structs> */" << EOL
		<< structs.str()
		<< "/* </Per pipeline structs> */" << EOL
		<< " void run() override {" << EOL
		<< "g_pipeline_name = \"Pipeline_" << number << "\";" << EOL
		<< "ThisThreadLocal& thread = query.get_thread_local<ThisThreadLocal>(*this);" << EOL
		<< "void* m_global_aggr_bucket = nullptr;" << EOL
		;

	impl << at_begin << EOL;

#ifdef TRACE
	for (auto& kc : key_checks) {
		impl << "size_t " << kc << " = 0;" << EOL;
	}
#endif
	const std::string pipeline_state("pipeline_flow_main_" + std::to_string(number));

	// flush buffers
	clite::Block* last_state = m_flow_gen->last_state();
	// last_state->append_plain("LOG_DEBUG(\"Last state\\n\");", "", {});
	ASSERT(last_state);

	{
		clite::Builder<clite::Block> builder(*last_state);
		builder << builder.CLITE_LOG("last state", {});
	}

	// generate buffer scheduling
	if (!m_buffers.empty()) {
		clite::Builder builder(*last_state);
		builder << builder.CLITE_LOG("Last state - Schedule Buffer", {});

		clite::ExprPtr any_overfull;

		for (size_t o=0; o<m_buffers.size(); o++) {
			auto& buf_info = m_buffers[o];
			auto buf_expr = builder.reference(buf_info.buffer->var);

			// find fullest buffer
			clite::ExprPtr this_buf_fullest = builder.function("!BUFFER_IS_EMPTY", buf_expr);
			auto count = builder.function("BUFFER_SIZE",
				buf_expr);

			for (size_t i=0; i<m_buffers.size(); i++) {
				if (i == o) {
					continue;
				}

				auto fullest = builder.function(">", count,
					builder.function("BUFFER_SIZE",
						builder.reference(m_buffers[i].buffer->var)));

				this_buf_fullest = builder.function("&&", fullest, this_buf_fullest);
			}

			builder << builder.predicated(this_buf_fullest,
				builder.CLITE_LOG("flush fullest " + buf_info.buffer->var->name + " last_state", {}));
			builder << builder.cond_branch(this_buf_fullest,
				buf_info.flush_state, clite::BranchLikeliness::Unknown);
		}
	}

	for (auto& buf_info : m_buffers) {
		clite::Builder<clite::Block> builder(*last_state);
		builder << builder.CLITE_LOG("last state: flush " + buf_info.buffer->var->name, {});
		ASSERT(buf_info.flush_state);
		m_data_gen->on_buffer_non_empty(buf_info.buffer, last_state,
			buf_info.flush_state, "final_flush");
	}

	// make sure buffers are flushed 
	for (auto& buf_info : m_buffers) {
		m_data_gen->assert_buffer_empty(buf_info.buffer, last_state);
	}

	clite::Builder builder(*last_state);

#ifdef TRACE
	for (auto& kc : key_checks) {
		builder << builder.plain("LOG_ERROR(\"%s: " + kc + " %d\\n\", g_pipeline_name, " + kc + ");");
	}
#endif

	if (!at_fin.empty()) {
		builder << builder.scope(at_fin);
	}

	builder
		<< builder.CLITE_LOG("fin", {})
		<< builder.exit()
		;


	auto reset_vars = [&] (auto& state, const std::string& dbg_name) {
		clite::Builder b(state);

		b << b.CLITE_LOG("reset variables '" + dbg_name + "'", {});
		// reset registered variables
		for (auto& reg_var : m_reset_vars) {
			auto& var = reg_var.var;

			b << reg_var.stmt;
		}
	};

	// reset variables for every flush_state
	size_t buf_id = 0;
	for (auto& buf_info : m_buffers) {
		auto& config = *buf_info.blend_config;
		ASSERT(!config.is_null());

		const std::string buf_name("buffer" + std::to_string(buf_id));
		auto fake_state = std::make_unique<clite::Block>("fake_" + buf_name);

		if (config.is_vectorized()) {
			reset_vars(*fake_state, buf_name);
		} else {
			clite::Builder builder(*fake_state);

			builder
				<< builder.comment("reset variables '" + buf_name + "'")
				<< builder.comment(config.computation_type)
				;
		}

		// move new nodes in front
		buf_info.flush_state->move_nodes_into(fake_state.get(), nullptr);

		buf_id++;
	}
	

	// flush last buffer from inflow
	ASSERT(m_inflow_back_edge);
	if (m_flush_back_edge) {
		// create fake state and insert it in front of 'm_inflow_back_edge'
		auto fake_state = std::make_unique<clite::Block>("fake");

		{
			clite::Builder builder(*fake_state);

			builder << builder.CLITE_LOG("m_flush_back_edge", {});

			reset_vars(*fake_state, "m_flush_back_edge");
		}


		m_current_blend->data_gen->on_buffer_non_empty(m_flush_buffer,
			fake_state.get(), m_flush_back_edge, "inflow_drain_last");


		{
			clite::Builder builder(*fake_state);

			// generate buffer scheduling
			if (!m_buffers.empty()) {
				builder << builder.CLITE_LOG("Schedule Buffer", {});

				clite::ExprPtr any_overfull;
				clite::StmtList flush_logic;

				for (size_t o=0; o<m_buffers.size(); o++) {
					auto& buf_info = m_buffers[o];
					auto buf_expr = builder.reference(buf_info.buffer->var);

					auto overfull = builder.function("BUFFER_CONTROL_SHALL_FLUSH",
						buf_expr);

					if (any_overfull) {
						any_overfull = builder.function("||",
							overfull, any_overfull);
					} else {
						any_overfull = overfull;
					}


					// find fullest buffer
					clite::ExprPtr this_buf_fullest;
					auto own_margin = builder.function("BUFFER_CONTROL_OVERFULL_MARGIN",
						buf_expr);

					for (size_t i=0; i<m_buffers.size(); i++) {
						if (i == o) {
							continue;
						}

						auto fullest = builder.function(">", own_margin,
							builder.function("BUFFER_CONTROL_OVERFULL_MARGIN",
								builder.reference(m_buffers[i].buffer->var)));

						if (this_buf_fullest) {
							this_buf_fullest = builder.function("&&",
								fullest, this_buf_fullest);
						} else {
							this_buf_fullest = fullest;
						}
					}

					flush_logic.emplace_back(builder.predicated(this_buf_fullest,
						builder.CLITE_LOG("flush fullest " + buf_info.buffer->var->name + " pre_inflow", {})));
					flush_logic.emplace_back(builder.cond_branch(this_buf_fullest,
						buf_info.flush_state, clite::BranchLikeliness::Unknown));
				}


				builder
					<< builder.predicated(any_overfull, flush_logic)
					<< builder.CLITE_LOG("Fallback to inflow_back_edge", {})
					;
			}
		}

		// copy before other nodes in 'm_inflow_back_edge'
		m_inflow_back_edge->move_nodes_into(fake_state.get(), nullptr);
	}


	// generate control flow
	{
		auto ctx = FlowGenerator::GenCtx {
			pipeline_state, decl, impl
		};
		m_flow_gen->generate(ctx);

		m_pipeline_id = number;
	}


	m_data_gen->end_eval_scope();

	impl<< " } /* run */" << EOL;
	impl << "}; /*Pipeline_" << number << "*/" << EOL;


	next << decl.str() << EOL;
	next << impl.str() << EOL;


	global_structs.clear();
}

BlendContext*
FujiCodegen::create_blend_context(const BlendConfig* const config, BlendContext* prev)
{
	ASSERT(config);
	auto ptr = new BlendContext(*this, config, prev);
	m_blend_context.push_back(ptr);
	return ptr;
}

void
FujiCodegen::gen_global_structs(std::ostringstream& out, std::ostringstream& reg,
	FujiCodegen::Record::Scope scope)
{
	for (auto& s : global_structs) {
		if (s.second.scope != scope) continue;

		const std::string type("S__" + s.first);

		const auto& items = s.second.items;
		out << "struct " << type;
		if (s.second.parent_class.size() > 0) {
			out << " : public " << s.second.parent_class;
		}
		out << "/*" << s.second.dbg_name << "*/";
		out << " {" << EOL;

		auto gen_attrs = [&] (bool is_meta) {
			for (auto& item : items) {
				auto& meta = item.second;
				if (meta.meta != is_meta) continue;

				out << "  ";

				if (meta.constant_expr) {
					out << "static constexpr ";
					ASSERT(meta.post_type.empty() && "Doesn't work yet. needs to refactoring");
				}

				out << meta.type << " ";

				if (meta.constant_expr) {
					out << meta.init;
				} else {
					out << item.first << meta.post_type;
				}

				out << ";" << EOL;
			}
		};

		out << "/* meta attributes */" << EOL;
		gen_attrs(true);
		out << "/* attributes */" << EOL;
		gen_attrs(false);

		out << "  " << type << "() {" << EOL;
			out << "dbg_name = \"" << type << "\";" << EOL;
		for (auto& item : items) {
			auto& meta = item.second;
			if (meta.init.empty()) continue;
			if (meta.constant_expr) continue;

			out << "    " << meta.init << ";" << EOL;
		}
		out << "  }" << EOL;

		out << "  ~" << type << "() {" << EOL;
		for (auto& item : items) {
			auto& meta = item.second;
			if (meta.deinit.empty()) continue;
			if (meta.constant_expr) continue;

			out << "    " << meta.deinit << ";" << EOL;
		}
		out << "  }" << EOL;

		out << "}; /* " << type << " */" << EOL;

		out << EOL;
#if 0
		out << type << " " << s.first << ";" << EOL;
		out << EOL;
		out << EOL;
#endif
	}
}

bool
FujiCodegen::concurrent_fsms_disabled() const
{
	return false;
}