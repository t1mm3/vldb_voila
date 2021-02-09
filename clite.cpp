#include "clite.hpp"
#include "runtime.hpp"
#include "utils.hpp"

using namespace clite;


const static bool alignment_simd = true;
const static int order_local_state_var = 1;

const static bool decomposed_state = true;
const static bool computed_goto = true;


Function::Function(const std::string& name, const ExprList& args, bool infix)
	: name(name), args(args), infix(infix)
{
	for (auto& arg : args) {
		ASSERT(arg);
	}
}

Cast::Cast(const std::string& type, const ExprPtr& expr)
	: type(type), expr(expr)
{
	ASSERT(type.size() > 1);
	ASSERT(expr);
}

Reference::Reference(const VarPtr& v)
	: var(v)
{
	ASSERT(v);
}

ArrayAccess::ArrayAccess(const ExprPtr& array, const ExprPtr& index)
	: array(array), index(index)
{
	ASSERT(array);
	ASSERT(index);
}

Assign::Assign(const ExprPtr& dest, const ExprPtr& e)
	: Stmt("assign"), dest(dest), expr(e)
{
	ASSERT(dest);
	ASSERT(e);
}


VarPtr
Fragment::clone_var(const std::string& name, const VarPtr& var)
{
	return std::make_shared<Variable>(name, var->type, var->scope,
		var->constant, var->default_value, var->dbg_name + "(clone)");
	ASSERT(false && "todo");
}

static bool
is_infix_fun(const std::string& f) {
	return str_in_strings(f, {"+", "*", "-", "/", "%",
		"<<", ">>", "&", "|", "^",
		"&&", "||",
		"<", ">", "<=", ">=", "==", "!="});
}

ExprPtr
Factory::function(const std::string& name, const ExprList& args) {
	for (auto arg : args) {
		ASSERT(arg);
	}
	bool infix = is_infix_fun(name);
	return std::make_shared<Function>(name, args, infix); 
}


static StmtList*
has_sub_statements(const StmtPtr& stmt)
{
	if (!stmt) return nullptr;

	if (auto scope = std::dynamic_pointer_cast<Scope>(stmt)) {
		return &scope->statements;
	}

	if (auto predicated = std::dynamic_pointer_cast<PredicatedStmt>(stmt)) {
		return &predicated->then;
	}

	return nullptr;
}

static std::string
stmt2str(const StmtPtr& stmt, bool full = true)
{
	if (!stmt) return "";

	if (auto assign = std::dynamic_pointer_cast<Assign>(stmt)) {
		return "Assign";
	}

	if (auto effect = std::dynamic_pointer_cast<Effect>(stmt)) {
		return "Effect";
	}

	if (auto branch = std::dynamic_pointer_cast<Branch>(stmt)) {
		std::string r(branch->cond ? "CondBr" : "UncondBr");

		if (!full) return r;

		std::string b(branch->dest ? branch->dest->dbg_name : "NULL");
		return r + std::string(" ") + b;
	}

	if (auto cmt = std::dynamic_pointer_cast<CommentStmt>(stmt)) {
		return "Comment";
	}

	if (auto plain = std::dynamic_pointer_cast<PlainStmt>(stmt)) {
		return "Plain";
	}

	if (auto inl_tar = std::dynamic_pointer_cast<InlineTarget>(stmt)) {
		return "InlineTarget";
	}

	if (auto scope = std::dynamic_pointer_cast<Scope>(stmt)) {
		std::string r("Scope");

		auto psub = has_sub_statements(stmt);
		ASSERT(psub);

		for (auto& s : *psub) {
			r += " " + stmt2str(s, full) + ";";
		}

		return r;
	}

	if (auto predicated = std::dynamic_pointer_cast<PredicatedStmt>(stmt)) {
		std::string r("Predicated");

		auto psub = has_sub_statements(stmt);
		ASSERT(psub);

		for (auto& s : *psub) {
			r += " " + stmt2str(s, full) + ";";
		}

		return r;
	}

	ASSERT(false);
	return "???";
}

void
Pass::recurse_stmt(const StmtPtr& stmt)
{
	if (!stmt) return;

	if (auto assign = std::dynamic_pointer_cast<Assign>(stmt)) {
		on_expr(assign->dest);
		on_expr(assign->expr);
		return;
	}

	if (auto effect = std::dynamic_pointer_cast<Effect>(stmt)) {
		on_expr(effect->expr);
		return;
	}

	if (auto branch = std::dynamic_pointer_cast<Branch>(stmt)) {
		on_expr(branch->cond);
		return;
	}

	if (auto cmt = std::dynamic_pointer_cast<CommentStmt>(stmt)) {
		return;
	}

	if (auto plain = std::dynamic_pointer_cast<PlainStmt>(stmt)) {
		return;
	}

	if (auto inl_tar = std::dynamic_pointer_cast<InlineTarget>(stmt)) {
		return;
	}


	if (auto scope = std::dynamic_pointer_cast<Scope>(stmt)) {
		auto psub = has_sub_statements(stmt);
		ASSERT(psub);
		for (auto& s : *psub) {
			on_stmt(s);
		}
		return;
	}

	if (auto predicated = std::dynamic_pointer_cast<PredicatedStmt>(stmt)) {
		if (predicated->cond) on_expr(predicated->cond);

		auto psub = has_sub_statements(stmt);
		ASSERT(psub);
		for (auto& s : *psub) {
			on_stmt(s);
		}
		return;
	}

	ASSERT(false && "Unreachable");
}

void
Pass::on_stmt(const StmtPtr& stmt)
{
	recurse_stmt(stmt);
}

void
Pass::recurse_expr(const ExprPtr& expr)
{
	if (!expr) return;

	if (auto fun = std::dynamic_pointer_cast<Function>(expr)) {
		for (auto& arg : fun->args) {
			on_expr(arg);
		}
		return;
	}

	if (auto ref = std::dynamic_pointer_cast<Reference>(expr)) {
#if 0
		auto& var = ref->var;

		if (var->default_value) {
			on_expr(var->default_value);
		}
		if (var->ctor_init) {
			on_expr(var->ctor_init);
		}
#endif
		return;
	}

	if (auto literal = std::dynamic_pointer_cast<Literal>(expr)) {
		return;
	}

	if (auto cast = std::dynamic_pointer_cast<Cast>(expr)) {
		on_expr(cast->expr);
		return;
	}

	if (auto arr = std::dynamic_pointer_cast<ArrayAccess>(expr)) {
		on_expr(arr->array);
		on_expr(arr->index);
		return;
	}

	ASSERT(false && "Unreachable");
}

void 
Pass::on_expr(const ExprPtr& expr)
{
	recurse_expr(expr);
}


struct OnVariablePass : Pass {
	std::unordered_set<VarPtr> variables;

	void on_var(const VarPtr& v) {
		variables.insert(v);
	}

	void on_expr(const ExprPtr& e) {
		recurse_expr(e);

		if (auto var = std::dynamic_pointer_cast<Reference>(e)) {
			on_var(var->var);
		}
	}

	void on_stmt(const StmtPtr& s) {
		recurse_stmt(s);
	}
};

struct OnVariableDepthPass : OnVariablePass {
	void on_var(const VarPtr& var) {
		variables.insert(var);

		if (var->default_value) {
			on_expr(var->default_value);
		}
		if (var->ctor_init) {
			on_expr(var->ctor_init);
		}

		for (auto& stmt : var->ctor_body) {
			on_stmt(stmt);
		}
	}
};

inline static std::string
branch_likely_prefix(BranchLikeliness l)
{
	switch (l) {
	case BranchLikeliness::Always:
	case BranchLikeliness::Likely:
		return "LIKELY(";
	case BranchLikeliness::Never:
	case BranchLikeliness::Unlikely:
		return "UNLIKELY(";
	default:
		return "(";
	}
}

std::string
CGen::fsm_set_state(const std::string& new_state) const
{
	std::ostringstream s;

	if (decomposed_state) {
		s << "_decomp_state[schedule_idx] =" << new_state << ";";
	} else {
		s << local_state_var << "." << fsm_state_var << " = " << new_state << ";";
	}
	return s.str();
}

std::string
CGen::fsm_get_state() const
{
	std::ostringstream s;

	if (decomposed_state) {
		s << "_decomp_state[schedule_idx]";
	} else {
		s << local_state_var << "." << fsm_state_var;
	}
	return s.str();
}

void
CGen::on_stmt(StmtContext& ctx, const StmtPtr& stmt)
{
	ASSERT(stmt);

	if (auto assign = std::dynamic_pointer_cast<Assign>(stmt)) {
#if 0
		ctx.impl << "ASSIGN("
			<< on_expr(ctx, assign->dest)
			<< ","
			<< on_expr(ctx, assign->expr)
			<< ");" << std::endl;
#else
		ctx.impl << on_expr(ctx, assign->dest) << " = "
			<< on_expr(ctx, assign->expr) << ";" << std::endl;
#endif
		auto x = on_expr(ctx, assign->dest);

#ifndef PERFORMANCE_MODE
		// impl << "std::cout << \"Assign to @\" << __LINE__ << \" '" << x << "' with \" << " << x << " << std::endl;" << std::endl;
#endif
 		return;
	}

	if (auto effect = std::dynamic_pointer_cast<Effect>(stmt)) {
		ctx.impl << on_expr(ctx, effect->expr) << ";" << std::endl;
		return;
	}

	if (auto branch = std::dynamic_pointer_cast<Branch>(stmt)) {
		if (branch->cond) {
			ctx.impl  << "if (" << branch_likely_prefix(branch->likely)
				<< on_expr(ctx, branch->cond) << ")" << ") {" << std::endl;
		}

		if (branch->is_exit) {
			gen_jump_exit(branch->threading);
		} else {
			gen_jump(branch->dest, branch->threading);
		}

		if (branch->cond) {
			ctx.impl << "}" << std::endl;
		}
		return;
	}

	if (auto cmt = std::dynamic_pointer_cast<CommentStmt>(stmt)) {
		ctx.impl << "/*" << cmt->text << "*/" << std::endl;
		return;
	}

	if (auto plain = std::dynamic_pointer_cast<PlainStmt>(stmt)) {
		ctx.impl << plain->text << std::endl;
		return;
	}

	if (auto inl_tar = std::dynamic_pointer_cast<InlineTarget>(stmt)) {
		ctx.impl << "/* INLINE TARGET */" << std::endl; 
		return;
	}

	if (auto scope = std::dynamic_pointer_cast<Scope>(stmt)) {
		auto psub = has_sub_statements(stmt);
		ASSERT(psub);
		for (auto& s : *psub) {
			on_stmt(ctx, s);
		}
		return;
	}

	if (auto pred = std::dynamic_pointer_cast<PredicatedStmt>(stmt)) {
		ctx.impl << "if (" << on_expr(ctx, pred->cond) << ") {" << std::endl;
		auto psub = has_sub_statements(stmt);
		ASSERT(psub);
		for (auto& s : *psub) {
			on_stmt(ctx, s);
		}
		ctx.impl << "}" << std::endl;
		return;
	}

	ASSERT(false && "Unreachable");
}

static bool ends_with(const std::string& s, const std::string& end) {
	if (s.length() < end.length()) {
		return false;
	}
	return !s.compare (s.length() - end.length(), end.length(), end);
}

std::string
CGen::on_expr(StmtContext& ctx, const ExprPtr& expr)
{
	ASSERT(expr);

	if (auto reference = std::dynamic_pointer_cast<Reference>(expr)) {
		const auto& var = reference->var;
		bool inlineable_constant = false; // var->constant;

		if (inlineable_constant) {
			return on_expr(ctx, var->default_value);
		}
		return get_var_id(var);
	}

	std::ostringstream s;

	if (auto literal = std::dynamic_pointer_cast<Literal>(expr)) {
		if (literal->is_string) {
			s << "\"";
		}

		s << literal->text;

		if (literal->is_string) {
			s << "\"";
		}

		return s.str();
	}

	if (auto cast = std::dynamic_pointer_cast<Cast>(expr)) {
		s << "(" << cast->type << ")(" << on_expr(ctx, cast->expr) << ")";
		return s.str();
	}

	if (auto arr = std::dynamic_pointer_cast<ArrayAccess>(expr)) {
		s << on_expr(ctx, arr->array) << "[" << on_expr(ctx, arr->index) << "]";
		return s.str();
	}

	if (auto function = std::dynamic_pointer_cast<Function>(expr)) {
		std::string f(function->name);
		if (f == "print") {
			f = "CLITE_PRINT";
		}
		if (function->infix) {
			ASSERT(function->args.size() == 2);
			
			s << "(" << on_expr(ctx, function->args[0]) << " " << f << " " << on_expr(ctx, function->args[1]) << ")";
		} else {
			std::string begin("(");
			std::string end(")");
			bool has_name = !f.empty();
			bool has_brackets = true;

			if (f == "") {
				has_brackets = false;
			}

			// creates init-list
			if (ends_with(f, "{}")) {
				ASSERT(f.size() >= 2);
				begin = f.substr(0, f.size()-1);
				ASSERT(begin.size() >= 1);
				// begin = "{";
				end = "}";
				has_name = false;
			}

			if (f == "wrap") {
				has_name = false;
				ASSERT(function->args.size() == 1);
			}

			if (has_name) {
				s << f;
			}

			if (has_brackets) {
			 	s << begin;
			}

			bool first = true;
			for (auto& arg : function->args) {
				if (!first) {
					s << ", ";
				}
				s << on_expr(ctx, arg);
				first = false;
			}

			if (has_brackets) {
				s << end;
			}
		}

		return s.str();
	}

	ASSERT(false && "Unreachable");
	return "";
}

void
CGen::declare_var(std::ostringstream& out, const VarPtr& var, StmtContext& ctx, 
	Level level, const std::string& dbg_cat)
{
	if ((!var->constant || var->prevent_promotion) &&
			(generated_variables.find(var->name) != generated_variables.end())) {

		// out << "/* skip " << var->name << "*/" << std::endl;
		return;
	}


	generated_variables.insert({var->name, level});

	out << "  ";

	if (var->constant) {
		if (var->type == "varchar") {
			out << " static const ";	
		} else {
			out << " const  ";
		}
	}

	out << var->type << " " << var->name;
	if (alignment_simd && var->type.size() > 2 && var->type[0] == '_' &&
			(var->type[1] == 'v' || var->type[1] == '_')) {

		auto t = var->type;
		int align = 0;

		if (t == "_v128") {
			align = 128;
		} else if (t == "_v256") {
			align = 256;
		} else if (t == "_v512") {
			align = 512;
		}
		if (t == "__mmask8") {
			align = 0;
		}
		printf("T=%s\n", t.c_str());
		align /= 8;

		if (align) {
			out << " alignas(" << align << ")";
		}
	}

	if (var->default_value) {
		out << " = " << on_expr(ctx, var->default_value);
	}

	if (var->ctor_init) {
		// ASSERT(var->scope == Variable::Scope::ThreadWide);
		out << "(" << on_expr(ctx, var->ctor_init) << ")";
	}
	out << ";" << "/*" << dbg_cat << "*/" << std::endl;

	if (var->scope == Variable::Scope::Local && num_parallel > 1) {
		ctx.impl
			<< "for (int i=0; i<" << num_parallel << "; i++) {" << std::endl
			<< "auto& " << local_state_var << " = all_local_states[i];" << std::endl
			;
	}

	for (auto& stmt : var->ctor_body) {
		on_stmt(ctx, stmt);
	}


	if (var->scope == Variable::Scope::Local && num_parallel > 1) {
		ctx.impl << "}" << std::endl;
	}

	if (var->type.rfind("S__", 0) == 0) {
		out << "objects.push_back({ __LINE__, &" << var->name << "});" << std::endl;
	}
}

std::string
CGen::get_var_id(const VarPtr& var) const
{
	ASSERT(var);

	auto it = generated_variables.find(var->name);

	ASSERT(it != generated_variables.end());

	switch (it->second) {
	case Level::LocalVar:
	case Level::ThreadWide:
		return var->name;
	case Level::LocalState:
		return local_state_var + "." + var->name;
	default:
		ASSERT(false);

		if (var->scope == Variable::Scope::ThreadWide) {
			return var->name;
		} else {
			ASSERT(var->scope == Variable::Scope::Local);

			if (var->_promoted_to_local) {
				return var->name;
			}

			return local_state_var + "." + var->name;
		}
	}
}

template<typename T>
static void
sort_vars(T& variables)
{
	// order by name
	std::sort(variables.begin(), variables.end(),
		[] (const VarPtr& _a, const VarPtr& _b) {
			const std::string var_prefix("__tmp");
			const auto& a = _a->name;
			const auto& b = _b->name;

			auto a_pos = a.rfind(var_prefix, 0);
			auto b_pos = b.rfind(var_prefix, 0);
			if (!a_pos && !b_pos) {
				// both are variables
				long a_id = std::stol(a.substr(var_prefix.size()));
				long b_id = std::stol(b.substr(var_prefix.size()));

				return a_id < b_id;
			}

			return std::lexicographical_compare(a.begin(), a.end(),
				b.begin(), b.end());
			});
}


#include <unordered_map>
#include <unordered_set>
#include <deque>


struct PruneCheckStatements : Pass {
	std::unordered_map<Block*, std::unordered_set<Block*>> reach_in;
	std::unordered_map<Block*, size_t> degree_in;
	std::unordered_map<Block*, std::unordered_set<Block*>> reach_out;
	std::unordered_map<Block*, size_t> degree_out;

	Block* block = nullptr;

	void on_expr(const ExprPtr& e) override {
		// recurse_expr(e);
	}

	void on_stmt(const StmtPtr& s) override {
		if (output) {
			LOG_DEBUG("PruneCheckStatements: %s\n", stmt2str(s).c_str());
		}

		if (auto branch = std::dynamic_pointer_cast<Branch>(s)) {
			reach_out[block].insert(branch->dest);
			degree_out[block]++;
			
			reach_in[branch->dest].insert(block);
			degree_in[branch->dest]++;

			if (output) {
				LOG_DEBUG("FSMOptimizer:   .. branch to %p '%s'\n", branch->dest,
					branch->dest ? branch->dest->dbg_name.c_str() : "NULL");
			}
		}

		if (output) {
			LOG_DEBUG("<Recurse>\n");
		}
		recurse_stmt(s);
		if (output) {
			LOG_DEBUG("</Recurse>\n");
		}
	}

	void on_block(Block* b) {
		block = b;
		for (const auto& stmt : b->statements) {
			on_stmt(stmt);
		}

		block = nullptr;
	}

	bool output = true;
};


static void
validate(Fragment& f, const std::string& name, const std::string& prefix = "FSM")
{
	size_t num_fail = 0;

	LOG_TRACE("%svalidate(%s): analyze:\n",
		prefix.c_str(), name.c_str());
	for (auto& s : f.blocks) {
		bool terminated = false;

		PruneCheckStatements check;
		check.output = false;

		check.on_block(s.get());

		LOG_TRACE("%svalidate(%s):   block %p '%s'\n",
			prefix.c_str(), name.c_str(), s.get(), s->dbg_name.c_str());

		for (auto& stmt : s->statements) {
			// no rules for comments
			if (auto cmt = std::dynamic_pointer_cast<CommentStmt>(stmt)) {
				continue;
			}

			LOG_TRACE("%svalidate(%s):   .. '%s'\n",
				prefix.c_str(), name.c_str(), stmt2str(stmt).c_str());

			if (terminated) {
				num_fail++;
				LOG_TRACE("%svalidate(%s):   .. fail\n",
					prefix.c_str(), name.c_str());
				continue;
			}

			if (auto branch = std::dynamic_pointer_cast<Branch>(stmt)) {
				if (branch->cond) {
					continue;
				}

				LOG_TRACE("%svalidate(%s):   .. Uncond branch to '%s' terminates block\n",
					prefix.c_str(), name.c_str(),
					branch->dest ? branch->dest->dbg_name.c_str() : "NULL");
				terminated = true;
				continue;
			}
		}
	}
}

struct BlockPass : Pass {
	Block* block = nullptr;

	virtual void on_block(Block* s) {
		block = s;
		for (auto& stmt : s->statements) {
			on_stmt(stmt);
		}
		block = nullptr;
	}
};

struct BranchGonePass : BlockPass {
	void on_expr(const ExprPtr& e) override {}

	void on_stmt(const StmtPtr& s) override {
		if (auto branch = std::dynamic_pointer_cast<Branch>(s)) {
			bool found = unreachable_blocks.find(branch->dest) != unreachable_blocks.end();

			if (found) {
				LOG_ERROR("Found unreachable block inside %p '%s'. When branching to %p '%s'\n",
					block, block->dbg_name.c_str(),
					branch->dest, branch->dest ? branch->dest->dbg_name.c_str() : "NULL");

				ASSERT(!found && "Found unreachable block");
			}
		}

		recurse_stmt(s);
	}

	
	BranchGonePass(const std::unordered_set<Block*>& bs) : unreachable_blocks(bs) {
	}

	const std::unordered_set<Block*> unreachable_blocks;
};


struct DcePass : BlockPass {
	static size_t get_runnable_num(const StmtList& statements) {
		for (size_t i=0; i<statements.size(); i++) {
			if (auto branch = std::dynamic_pointer_cast<Branch>(statements[i])) {
				if (!branch->cond) {
					return i+1;
				}
			}
		}

		return statements.size();
	}

	void prune_statements(StmtList& statements) const {
		size_t max = get_runnable_num(statements);

		if (statements.size() <= max) {
			ASSERT(statements.size() == max);
			return;
		}

		if (output) {
			LOG_DEBUG("DcePass: Block %p '%s': Pruning %d statement(s)\n",
				block, block->dbg_name.c_str(), 
				(int)statements.size() - (int)max);
		}

		first_n_inplace(statements, max);
	}

	void on_stmt(const StmtPtr& s) override {
		if (auto scope = std::dynamic_pointer_cast<Scope>(s)) {
			prune_statements(scope->statements);
		}

		recurse_stmt(s);
	}

	void on_block(Block* s) override {
		block = s;
		prune_statements(s->statements);

		BlockPass::on_block(s);
	}

	bool output = false;
};




void
CGen::generate(Fragment& fragm, GenCtx& ctx)
{
	block_mapping.clear();
	block_vars.clear();
	generated_variables.clear();

	const static bool optimize_states = true;
	const static bool optimize_variables = true;

	validate(fragm, "before");
	if (optimize_states) {
		size_t num_pruned;
		size_t round = 0;

		// remove states
		do {
			round++;
			num_pruned = prune_states(fragm, round, nullptr);
			validate(fragm, "after" + std::to_string(round));
		} while (num_pruned != 0);

#if 1
		std::unordered_map<Block*, size_t> call_inline_count;
		// inline into call sites

		for (int i=0; i<4; i++) {
			round++;
			num_pruned = prune_states(fragm, round, &call_inline_count);
			validate(fragm, "final" + std::to_string(round));			
		}
#endif

#if 1
		do {
			round++;
			num_pruned = prune_states(fragm, round, nullptr);
			validate(fragm, "after" + std::to_string(round));
		} while (num_pruned != 0);
#endif
	}

	// find variable usage
	std::unordered_map<BlockPtr, std::unordered_set<VarPtr>> block_vars;
	std::unordered_map<VarPtr, std::unordered_set<BlockPtr>> var2blocks;
	std::unordered_set<VarPtr> all_variables;

	for (auto& b : fragm.blocks) {
		OnVariablePass on_var;

		for (auto& stmt : b->statements) {
			on_var.on_stmt(stmt);
		}

		for (auto& v : on_var.variables) {
			block_vars[b].insert(v);
			var2blocks[v].insert(b);
			all_variables.insert(v);
		}
	}

	std::ostringstream state_decl;
	std::ostringstream state_ctor_init;
	std::ostringstream thread_decl;


	std::vector<VarPtr> dependent_vars;
	{
		std::unordered_set<VarPtr> vars;

		LOG_TRACE("All vars:\n");
		OnVariableDepthPass on_var;

		for (auto& v : all_variables) {
			LOG_TRACE("%s\n", v->name.c_str());
			on_var.on_var(v);
		}

		LOG_TRACE("New vars:\n")
		for (auto& var : on_var.variables) {
			if (all_variables.find(var) != all_variables.end()) {
				continue;
			}

			dependent_vars.push_back(var);

			LOG_TRACE("%s\n", var->name.c_str());
		}

		sort_vars(dependent_vars);
	}

	LOG_TRACE("Clite::generate\n");

	const size_t static_block_offset = 3;

	for (size_t block_id=0; block_id<fragm.blocks.size(); block_id++) {
		auto& block = fragm.blocks[block_id];

		LOG_TRACE("block id %d  block %p contained %d\n",
			block_id, block.get(),
			block_mapping.find(block.get()) != block_mapping.end());

		ASSERT(block_mapping.find(block.get()) == block_mapping.end());

		const std::string id_str(num_parallel > 1 ?
			std::to_string(block_id + static_block_offset) : "block_" + std::to_string(block_id));

		block_mapping[block.get()] = id_str;
	}




	const std::string local_state_type(unique_prefix + "_LocalState");

	state_decl << "struct " << " " << local_state_type << " {" << std::endl;

	std::ostringstream pre_impl;

	if (num_parallel > 1) {
		if (decomposed_state) {
			impl << "int64_t _decomp_state[" << num_parallel << "] = {0};" << std::endl;
		} else {
			state_decl << "int64_t " << fsm_state_var << " = 0;" << std::endl;
		}

		pre_impl<< local_state_type << " all_local_states[" << num_parallel << "] alignas(64);" << std::endl;
		pre_impl<< "register " << local_state_type << "* ptr_local_state = &all_local_states[0];" << std::endl;
	} else {
		pre_impl << unique_prefix << "_LocalState" << " " << local_state_var <<  "  alignas(64);" << std::endl;
	}

	if (num_parallel > 1) {
		impl<< "size_t schedule_counter = 0;" << std::endl
			<< "size_t schedule_num_running = " << num_parallel << ";" << std::endl
			<< "size_t num_count = 0;" << std::endl
			<< "int64_t schedule_lock_count = 0;" << std::endl
			;
	}

	impl<< "size_t schedule_idx = 0;" << std::endl;
	// impl << "g_lwt_id = 0; " << std::endl;
	if (num_parallel > 1) {
		std::ostringstream next;
		next << "if (LIKELY(schedule_counter > 32)) { schedule_idx=(schedule_counter++) % " << num_parallel<< " ; }"
			"else { "
				<< "if (schedule_counter > 13) { schedule_idx=(schedule_counter++ * 3) % " << num_parallel<< "; } "
				<< "else { schedule_idx=(schedule_counter++ * 5) % " << num_parallel<< "; }"
			<< "}"
			<< "ptr_local_state = &all_local_states[schedule_idx];";

		impl
			<< "#define SCHEDULE_NEXT(THREADING) \\" << std::endl
			<< "  /* next state set outside */\\" << std::endl
			<< "  if (THREADING > 0) { \\" << std::endl
			<< "    if (LIKELY(schedule_lock_count == 0)) { " << next.str() << " }\\" << std::endl;
		if (computed_goto) {
			impl<< " goto *dispatch_table[" << fsm_get_state() << "]; \\" << std::endl;
		}
		impl<< "  }" << std::endl;


		impl<< "#define SCHEDULE_NEXT_SET_STATE(THREADING, NEXT_STATE) \\" << std::endl;
		if (!computed_goto) {
			impl<< "  " << fsm_set_state("NEXT_STATE") << "\\"<< std::endl;
		}
		impl<< "  /* also set next state to NEXT_STATE */\\" << std::endl
			<< "  if (THREADING > 0 && LIKELY(schedule_lock_count == 0)) { \\" << std::endl;
		if (computed_goto) {
			impl<< "  " << fsm_set_state("NEXT_STATE") << "\\"<< std::endl;
		}
		impl<< "    " << next.str() << ";\\" << std::endl;
		if (computed_goto) {
			impl<< "    goto *dispatch_table[" << fsm_get_state() << "]; \\" << std::endl;
		} else {
			impl<< "    break; \\" << std::endl;
		}
		impl<< "  } else {\\" << std::endl;
		if (computed_goto) {
			// TODO: not sure why we cannot optimize this away
			impl<< "    goto cgoto_##NEXT_STATE; /* KNOWN branch */\\" << std::endl;
		} else {
			impl<< "    /* cannot optimize in SWITCH */\\" << std::endl;
			impl<< "    break; \\" << std::endl;
		}
		impl<< "  }" << std::endl
			<< std::endl;
		
		if (computed_goto) {

			impl << "const void* dispatch_table[] = { &&cgoto_0, &&cgoto_1, &&cgoto_2 ";

			for (size_t block_id=0; block_id<fragm.blocks.size(); block_id++) {
				auto& block = fragm.blocks[block_id];
				const auto id_str = block_mapping[block.get()];
				impl << ", &&cgoto_" << id_str;
			}

			impl << "};" << std::endl;

			impl <<"SCHEDULE_NEXT(0);" << std::endl;
		}

		impl<< "while(1) {" << std::endl
			<< "/* Schedule next LWT */" << std::endl
			<< "/* SCHEDULE_NEXT(); */" << std::endl
			<< "DBG_ASSERT(schedule_idx >= 0 && schedule_idx < " << num_parallel << ");" << std::endl
			<< std::endl
			<< "LOG_TRACE(\"LWT %d@%d: RUN %d local_state %p\\n\", schedule_idx, __LINE__, "
				<< fsm_get_state() << ", ptr_local_state);" << std::endl;

		// impl << "g_lwt_id = schedule_idx;" << std::endl;
		if (computed_goto) {

		} else {
			impl << "switch (" << fsm_get_state() << ") {" << std::endl;
		}

		auto new_block = [&] (const auto& id, const auto& msg) {
			if (computed_goto) {
				impl<< "  cgoto_" << id << ": /*" << msg << "*/" << std::endl;
			} else {
				impl<< "  case " << id << ": /*" << msg << "*/" << std::endl;
			}
		};

		new_block("0", "INIT");
		impl
			// << fsm_set_state(std::to_string(static_block_offset)) << std::endl
			<< "LOG_TRACE(\"LWT %d@%d: INIT\\n\", schedule_idx, __LINE__);" << std::endl
			<< "SCHEDULE_NEXT_SET_STATE(0, " << static_block_offset << ");" << std::endl;
		if (computed_goto) {
		} else  {
			impl << " break; ";
		}
		impl << std::endl;

		new_block("1", "END");
		impl
			<< "ASSERT(schedule_num_running > 0); schedule_num_running--; " << std::endl
			<< "LOG_TRACE(\"LWT %d@%d: END. %d running\\n\", schedule_idx, __LINE__, schedule_num_running);" << std::endl
			<< "SCHEDULE_NEXT_SET_STATE(1, 2); " << std::endl
			;
		if (computed_goto) {

		} else {
			impl << " break; " << std::endl;
		}
			
		new_block("2", "ZOMBIE");
		impl
			<< "LOG_TRACE(\"LWT %d@%d: ZOMBIE. %d running\\n\", schedule_idx, __LINE__, schedule_num_running);" << std::endl
			<<" if (!schedule_num_running) { goto END;} " << std::endl
			<< "SCHEDULE_NEXT(1); "  << std::endl
			;
		if (!computed_goto) {
			impl << " break; " << std::endl;
		}


	}

	StmtContext stmt_ctx(impl);
	std::ostringstream decl_stmts;
	StmtContext decl_stmt_ctx(decl_stmts);

	LOG_DEBUG("New vars:\n")
	for (auto& var : dependent_vars) {
		LOG_DEBUG("%s\n", var->name.c_str());
		declare_var(state_decl, var, decl_stmt_ctx,
			Level::LocalState, "global");
	}

	std::vector<VarPtr> delayed_local_struct_var;


	for (size_t block_id=0; block_id<fragm.blocks.size(); block_id++) {
		auto& block = fragm.blocks[block_id];
		const auto id_str = block_mapping[block.get()];

		impl << std::endl;

		std::ostringstream dbg_txt_new_block;
		dbg_txt_new_block << "/* new block '" << id_str << "' or '" << block->dbg_name << "' */" << std::endl;

		if (num_parallel > 1) {
			if (computed_goto) {
				impl << "cgoto_" << id_str << ": ";
			} else {
				impl << "case " << id_str << ": ";
			}
		} else {
			impl << id_str << " : ";
		}
		impl<< "{" << std::endl;

		if (num_parallel > 1) {
			impl << local_state_type << "& local_state = *ptr_local_state;" << std::endl;
		}

		impl << "LOG_TRACE(\"%s LWT %d@%d: block '" << id_str << "' or '" << block->dbg_name << "'\\n\", g_pipeline_name, schedule_idx, __LINE__);" << std::endl;

		LOG_TRACE("insert %p\n", block.get());

		// generate state
		std::vector<VarPtr> variables;
		{
			auto& bvars = block_vars[block];

			variables.reserve(bvars.size());
			for (auto&& v : bvars) {
				variables.emplace_back(v);
			}

			sort_vars(variables);

			dbg_txt_new_block << "/* BLOCK VARS" << std::endl;
			LOG_DEBUG("Block vars:\n");
			bool first = true;
			for (auto& var : variables) {
				(void)var;
				LOG_DEBUG("%s\n", var->name.c_str());

				if (!first) {
					dbg_txt_new_block << ", ";
				}
				dbg_txt_new_block << var->name;
				first = false;
			}
			dbg_txt_new_block << "*/" << std::endl;
		}

		impl << dbg_txt_new_block.str();
		state_decl << dbg_txt_new_block.str();
		thread_decl << dbg_txt_new_block.str();

		for (auto& var : variables) {
			auto blocks = var2blocks.find(var);
			ASSERT(blocks != var2blocks.end());

			bool promote_to_local_var = false;

			if (var->constant) {
				promote_to_local_var = true;
			}

			if (!promote_to_local_var &&
					var->scope == Variable::Scope::Local && 
					blocks->second.size() <= 1) {
				ASSERT(blocks->second.size() == 1);
				ASSERT(blocks->second.find(block) != blocks->second.end());

				// can only be used in this block
				promote_to_local_var = true;
			}

			if (var->prevent_promotion || !optimize_variables) {
				promote_to_local_var = false;
			}

			if (promote_to_local_var) {
				var->_promoted_to_local = true;
				declare_var(impl, var, decl_stmt_ctx,
					Level::LocalVar, "local_prm");
			} else if (var->scope == Variable::Scope::Local) {
				if (order_local_state_var) {
					delayed_local_struct_var.push_back(var);
				} else {
					declare_var(state_decl, var, decl_stmt_ctx,
						Level::LocalState, "local_dflt");
				}
			} else if (var->scope == Variable::Scope::ThreadWide) {
				declare_var(thread_decl, var, decl_stmt_ctx,
					Level::ThreadWide, "local_tw");
			} else {
				ASSERT(false);
			}
		}

		if (order_local_state_var == 1) {
			std::sort(std::begin(delayed_local_struct_var),
				std::end(delayed_local_struct_var),
				[] (const auto& var_a, const auto& var_b) {
					return var_a->type > var_b->type;
				});
			for (auto& var : delayed_local_struct_var) {
				declare_var(state_decl, var, decl_stmt_ctx,
					Level::LocalState, "local_dflt");
			}
		}

		// code
		for (auto& stmt : block->statements) {
			on_stmt(stmt_ctx, stmt);
		}

		impl << std::endl;
		impl << "ASSERT(false && \"Unreachable\");" << std::endl;
		impl << " }" << std::endl;
	}

	if (num_parallel > 1) {
		if (!computed_goto) {
			impl << "} /* switch */" << std::endl;
		}

		impl << "} /* while */" << std::endl;
		impl << "#undef SCHEDULE_NEXT" << std::endl;
		impl << "#undef SCHEDULE_NEXT_SET_STATE" << std::endl;
	}

	impl << "ASSERT(false && \"Unreachable\");" << std::endl;
	impl << "END: LOG_TRACE(\"num schedules %d\\n\", schedule_counter); return;" << std::endl;

	state_decl << "};" << std::endl;

	std::ostringstream& final_impl = ctx.impl;
	std::ostringstream& final_decl = ctx.decl;

	final_impl
		<< "/* thread declare */" << std::endl
		<< thread_decl.str() << std::endl
		<< "/* pre impl */" << std::endl
		<< pre_impl.str() << std::endl
		<< "/* init body */" << std::endl
		<< decl_stmts.str() << std::endl
		<< "/* implementation */" << std::endl
		<< impl.str() << std::endl; 

	final_decl
		<< "/* state declare */" << std::endl
		<< state_decl.str() << std::endl;
}

void
CGen::gen_jump(Block* b, BranchThreading threading)
{
	ASSERT(block_mapping.find(b) != block_mapping.end());
	if (num_parallel > 1) {
		impl << "SCHEDULE_NEXT_SET_STATE("
			<< threading << ", " << block_mapping[b] << ");";
		if (!computed_goto) {
			impl << "break;";
		}
	} else {
		impl << "goto " << block_mapping[b] <<  ";";
	}

	impl << "/* '" << b->dbg_name << "' */"
		<< std::endl;
}

void
CGen::gen_jump_exit(BranchThreading threading)
{
	if (num_parallel > 1) {
		impl << "SCHEDULE_NEXT_SET_STATE(1, 1)";
		if (!computed_goto) {
			impl << "break;";
		}
	} else {
		impl << "goto END;" << std::endl;
	}
}

static bool should_inline(Block* blk)
{
	if (!blk) {
		return false;
	}

	for (auto& stmt : blk->statements) {
		if (auto inl_tar = std::dynamic_pointer_cast<InlineTarget>(stmt)) {
			return true;
		}
	}

	return false;
} 


struct HasBackEdgePass : BlockPass {
	bool has_back_edge = false;
	std::unordered_set<Block*> nodes;

	void on_stmt(const StmtPtr& stmt) override {
		if (has_back_edge) {
			return;
		}

		if (auto go = std::dynamic_pointer_cast<Branch>(stmt)) {
			if (nodes.find(go->dest) != nodes.end()) {
				has_back_edge = true;
				return;
			}
		}

		recurse_stmt(stmt);
	}
};

static bool has_back_edge(Block* me, Block* other)
{
	HasBackEdgePass pass;
	if (!me || !other) {
		return false;
	}

	pass.nodes.insert(me);

	pass.on_block(other);
	return pass.has_back_edge;
}

#include <stack>


struct CountGotoPass : BlockPass {
	std::unordered_map<Block*, size_t> counts;

	void on_stmt(const StmtPtr& stmt) override {
		if (auto go = std::dynamic_pointer_cast<Branch>(stmt)) {
			counts[go->dest]++;
		}

		recurse_stmt(stmt);
	}
};

struct CountStatementsPass : BlockPass {
	size_t count = 0;

	void on_stmt(const StmtPtr& stmt) override {
		count++;

		recurse_stmt(stmt);
	}
};


struct InlineSelectPass : Pass {
	struct Selected {
		StmtList* surr_statements;
		StmtPtr stmt;
		Branch* node_goto;
		const bool remove;
		const size_t num_in_edges;
		const size_t num_out_edges;

		Selected(StmtList* ss, const StmtPtr& s, Branch* go, bool remove,
			size_t num_in_edges, size_t num_out_edges)
		 : surr_statements(ss), stmt(s), node_goto(go), remove(remove),
		 num_in_edges(num_in_edges), num_out_edges(num_out_edges) {}
	};

	std::unique_ptr<Selected> selected;
private:
	const std::unordered_set<Block*>& m_removed;
	const PruneCheckStatements& m_check_pass;
	std::unordered_map<Block*, size_t>* m_copy_inline_count;
	const std::unordered_set<Block*>& m_copy_done;
	size_t m_block_count;
	CountGotoPass m_goto_counts;

	struct Stack {
		StmtList* statements;
	};
	std::stack<Stack> m_stack;

	Block* m_block = nullptr;

	const int64_t kInlineMaxLines = 100;
	const int64_t kInlineMaxDepth = 10;

	void on_stmt(const StmtPtr& stmt) override {
		auto psub = has_sub_statements(stmt);

		if (psub && m_stack.size() <= kInlineMaxDepth) {
			m_stack.push(Stack {psub} );
			for (auto& s : *psub) {
				on_stmt(s);
			}
			m_stack.pop();
		}

		if (auto go = std::dynamic_pointer_cast<Branch>(stmt)) {
			const auto& in_degree = m_check_pass.degree_in;
			const auto& out_degree = m_check_pass.degree_in;

			if (go->is_exit || go->threading == BranchThreading::MustYield) {
				return;
			}

			if (m_removed.find(go->dest) != m_removed.end()) {
				return;
			}

			if (go->dest == m_block) {
				// self-edge
				return;
			}

			if (has_back_edge(go->dest, go->dest)) {
				return;
			}

			if (has_back_edge(m_block, go->dest)) {
				return;
			}

			if (m_goto_counts.counts[go->dest] > 1) {
				return;
			}

			int64_t in_edges = 0;
			auto in_edges_it = in_degree.find(go->dest);
			if (in_edges_it != in_degree.end()) {
				in_edges = in_edges_it->second;
			}

			if (in_edges > 1 && !m_copy_inline_count) {
				return;
			}

			if (m_copy_inline_count) {
				auto& call_inline_count = *m_copy_inline_count;
				if (call_inline_count[go->dest] > 4) {
					return;
				}
			}

#if 1
			CountStatementsPass counter;
			counter.on_block(go->dest);
			if (counter.count > kInlineMaxLines /* || counter.count > m_block_count */) {
				// only inline smaller pieces of code into bigger ones
				return;
			}
#endif
			if (!selected ||
					(!m_copy_inline_count && go->likely > selected->node_goto->likely) ||
					(m_copy_inline_count && should_inline(go->dest))) {

				int64_t out_edges = 0;
				auto out_edges_it = out_degree.find(go->dest);
				if (out_edges_it != out_degree.end()) {
					out_edges = out_edges_it->second;
				}

				selected = std::make_unique<Selected>(
					m_stack.top().statements,
					stmt,
					go.get(),
					(!m_copy_inline_count) && in_edges <= 1,
					in_edges,
					out_edges
				);

				ASSERT(go->dest != m_block);
			}
		}
	}

public:
	void on_block(Block* block) {
		LOG_TRACE("Select on block %p\n", block);
		if (m_copy_done.find(block) != m_copy_done.end()) {
			return;
		}
		m_goto_counts.on_block(block);

		CountStatementsPass counter;
		counter.on_block(block);
		m_block_count = counter.count;


		m_block = block;

		m_stack.push(Stack { &block->statements });
		for (auto& s : block->statements) {
			on_stmt(s);
		}
		m_stack.pop();

		ASSERT(m_stack.empty());

		m_block = nullptr;
	}

	InlineSelectPass(const std::unordered_set<Block*>& removed, const PruneCheckStatements& check_pass,
		const std::unordered_set<Block*>& copy_done, std::unordered_map<Block*, size_t>* copy_inline_count)
	 : m_removed(removed), m_check_pass(check_pass), m_copy_inline_count(copy_inline_count), m_copy_done(copy_done) {}
};


size_t
CGen::prune_states(Fragment& gen, size_t round, std::unordered_map<Block*, size_t>* copy_inline_count)
{
	bool copy_inline = !!copy_inline_count;

	size_t num_pruned = 0;
	std::vector<BlockPtr> opt_states;

	LOG_TRACE("FSMOptimizer: prune states for fragment '%s' round %d copy_inline %d\n",
		gen.dbg_name.c_str(), (int)round, (int)copy_inline);


	LOG_TRACE("FSMOptimizer: eliminate dead code:\n");
	DcePass dce_pass;
	dce_pass.output = true;

	for (auto& s : gen.blocks) {
		LOG_TRACE("FSMOptimizer:   block %p '%s'\n", s.get(), s->dbg_name.c_str());
		dce_pass.on_block(s.get());
	}


	LOG_TRACE("FSMOptimizer: analyze:\n");
	std::deque<Block*> todo;
	{
		// from state we can reach 'reach_out', 'reach_in' are the back-edges
		PruneCheckStatements check_pass;
		check_pass.output = false;

		for (auto& s : gen.blocks) {
			LOG_TRACE("FSMOptimizer:   block %p '%s'\n", s.get(), s->dbg_name.c_str());
			check_pass.on_block(s.get());
		}

		auto& in_degree = check_pass.degree_in;
		auto& out_degree = check_pass.degree_out;

		if (copy_inline) {
			std::vector<BlockPtr> blocks;
			for (auto& s : gen.blocks) {
				blocks.push_back(s);
			}

			std::sort(blocks.begin(), blocks.end(), [&] (const BlockPtr& a, const BlockPtr& b) {
				return out_degree[a.get()] > out_degree[b.get()];
			});

			for (auto& s : blocks) {
				todo.push_back(s.get());
			}
		} else {
			for (auto& s : gen.blocks) {
				if (in_degree[s.get()] > 1) {
					todo.push_back(s.get());		
					continue;
				}
			}		
		}
	}

	LOG_TRACE("FSMOptimizer: inline:\n");
	std::unordered_set<Block*> removed;

	while (!todo.empty()) {
		Block* s = todo.front();
		todo.pop_front();

		std::unordered_set<Block*> copy_done;


		PruneCheckStatements check_pass;
		check_pass.output = false;

		for (auto& s : gen.blocks) {
			LOG_TRACE("FSMOptimizer:   block %p '%s'\n", s.get(), s->dbg_name.c_str());
			check_pass.on_block(s.get());
		}

		LOG_TRACE("FSMOptimizer:   pop(%s) -> in %d, out %d\n",
			s ? s->dbg_name.c_str() : "NULL",
			(int)in_degree[s], (int)out_degree[s]);

		if (removed.find(s) != removed.end()) {
			// already removed
			LOG_TRACE("FSMOptimizer:   .. already removed\n");
			continue;
		}

		// inline until everything possible is inlined with in-degree <= 1
		while (1) {

			InlineSelectPass select(removed, check_pass, copy_done, copy_inline_count);

			select.on_block(s);

			if (!select.selected) {
				break;
			}

			int64_t best_node_idx = -1;
			Branch* best_node_goto = select.selected->node_goto;
			bool best_remove = select.selected->remove;
			StmtList* src_statements = select.selected->surr_statements;

			ASSERT(src_statements);

			// find best node idx
			for (size_t i=0; i<src_statements->size(); i++) {
				auto& ss = *src_statements;
				if (ss[i].get() == select.selected->stmt.get()) {
					best_node_idx = i;
					break;
				}
			}

			ASSERT(best_node_idx >= 0);

			copy_done.insert(best_node_goto->dest);

			// must inline all occurnces of best->dest

			// merge 'best' into 's'
			ASSERT(!best_node_goto->is_exit);
			ASSERT(best_node_goto->threading != BranchThreading::MustYield);

			auto target = best_node_goto->dest;

			ASSERT(target);
			ASSERT(src_statements);
			ASSERT(best_node_idx >= 0 && best_node_idx <= (int64_t)src_statements->size());

			std::string prefix(best_node_goto->cond ? "COND" : "UNCOND");
			prefix += std::string(best_remove ? " REMOVE " : " DUPLI");

			StmtList res;
			auto& in = *src_statements;
			const auto& replace = target->statements;

			ASSERT(src_statements != &target->statements);

			for (auto& m : replace) {
				ASSERT(m);
				// LOG_DEBUG("%s inline: %s\n", prefix.c_str(), stmt2str(m).c_str());
				if (auto go = std::dynamic_pointer_cast<Branch>(m)) {
					ASSERT(go->dest != s);
					ASSERT(go.get() != best_node_goto);
				}
			}

			if (copy_inline_count) {
				LOG_ERROR("INLINE target %p target statements %p %d, self %p %d\n",
					target, &target->statements, target->statements.size(),
					src_statements, src_statements->size());
			}
			ASSERT(target != s);

			for (int i=0; i<best_node_idx; i++) {
				ASSERT(in[i]);
				res.push_back(std::move(in[i]));
			}

			Factory f;

			if (best_node_goto->cond) {
				StmtList inlined;

				inlined.emplace_back(f.comment("PRUNE: inline " + prefix + " begin '" + target->dbg_name + "'"));

				for (auto& m : replace) {
					ASSERT(m);
					// LOG_DEBUG("%s inline: %s\n", prefix.c_str(), stmt2str(m).c_str());
					inlined.push_back(m);
				}

				inlined.emplace_back(f.comment("PRUNE: inline " + prefix + " end '" + target->dbg_name + "'"));

				res.emplace_back(f.predicated(best_node_goto->cond, std::move(inlined)));
			} else {
				res.emplace_back(f.comment("PRUNE: inline " + prefix + " begin '" + target->dbg_name + "'"));

				for (auto& m : replace) {
					ASSERT(m);
					// LOG_DEBUG("%s inline: %s\n", prefix.c_str(), stmt2str(m).c_str());
					res.push_back(m);
				}

				res.emplace_back(f.comment("PRUNE: inline " + prefix + " end '" + target->dbg_name + "'"));
			}


			for (size_t i=best_node_idx+1; i < in.size(); i++) {
				ASSERT(in[i]);
				res.emplace_back(std::move(in[i]));
			}
#if 1
			for (auto& r : res) {
				ASSERT(r.get() != best_node_goto);
			}
#endif
			*src_statements = std::move(res);

			for (auto& ns : *src_statements) {
				ASSERT(ns);
			}

			if (copy_inline_count) {
				auto& call_inline_count = *copy_inline_count;
				call_inline_count[target]++;
			}

			if (best_remove) {
				LOG_TRACE("FSMOptimizer:   .. pruned %s @ %d\n",
					target->dbg_name.c_str(), (int)best_node_idx);
				num_pruned++;

				removed.insert(target);					

				BranchGonePass gone_pass({target});
				gone_pass.on_block(s);
			} else {
				LOG_TRACE("FSMOptimizer:   .. copied %s @ %d\n",
					target->dbg_name.c_str(), (int)best_node_idx);
			}
		} // while(1)
	} // while

	size_t old_num = gen.blocks.size();
	size_t num_empty = 0;


	LOG_TRACE("FSMOptimizer: create\n");
	for (auto& s : gen.blocks) {
		// did we inline this one?
		if (removed.find(s.get()) != removed.end()) {
			LOG_TRACE("FSMOptimizer:   removed %p '%s' in-degree %d\n",
				s.get(), s->dbg_name.c_str(), (int)in_degree[s.get()]);
			continue;
		}

#if 1
		if (s->statements.empty()) {
			num_empty++;
			LOG_TRACE("FSMOptimizer:   skipping %p '%s' in-degree %d\n",
				s.get(), s->dbg_name.c_str(), (int)in_degree[s.get()]);
			continue;
		}
#endif
		LOG_TRACE("FSMOptimizer:   add %p '%s' in-degree %d\n",
			s.get(), s->dbg_name.c_str(), (int)in_degree[s.get()]);
		opt_states.push_back(s);		
	}


	LOG_TRACE("FSMOptimizer: check for existence of invalid jumps\n")
	{
		BranchGonePass gone_pass(removed);

		for (auto& s : opt_states) {
			gone_pass.on_block(s.get());
		}
	}



	gen.blocks = opt_states;

	LOG_TRACE("FSMOptimizer: removed %d (%d) blocks of which %d are empty-> resulting in %d blocks\n",
		(int)(old_num - gen.blocks.size()), (int)num_pruned, (int)num_empty,
		(int)gen.blocks.size());

	return num_pruned;
}

VarPtr 
Fragment::new_var(const std::string& name, const std::string& type, Variable::Scope scope,
	bool constant, const std::string& default_value, const std::string& dbg_name)
{
	Factory f;
	ExprPtr dflt;
	if (!default_value.empty()) {
		dflt = f.literal_from_str(default_value);
	}

	bool created = false;
	auto var = try_new_var(created, name, type, scope, constant, dflt, dbg_name);
	ASSERT(created && "Must not exist");
	ASSERT(var);
	return var;
}