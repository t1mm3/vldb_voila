#ifndef H_CLITE
#define H_CLITE

#include <string>
#include <sstream>
#include <memory>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include "utils.hpp"

namespace clite {
struct CliteNode {
	const std::string dbg_descr;

	CliteNode(const std::string& dbg_descr) : dbg_descr(dbg_descr) {

	}

	virtual ~CliteNode() {}
};


struct Expr;
typedef std::shared_ptr<Expr> ExprPtr;

struct Stmt;
typedef std::shared_ptr<Stmt> StmtPtr;
typedef std::vector<StmtPtr> StmtList;

struct Variable : CliteNode {
	const std::string name;
	std::string type;

	enum Scope {
		Local,
		ThreadWide
	};

	const Scope scope;
	std::string dbg_name;
	bool constant;
	bool prevent_promotion = false;
	ExprPtr default_value;


	ExprPtr ctor_init; //! Initialize with var(INIT)
	StmtList ctor_body;

	bool outside = false;

	Variable(const std::string& name, const std::string& type, Scope scope,
			bool constant, const ExprPtr& default_value, const std::string& dbg_name)
	 : CliteNode("var '" + name + "'"), name(name), type(type), scope(scope),
	 	dbg_name(dbg_name), constant(constant),
	 	default_value(default_value) {}

	bool _promoted_to_local = false;
};

typedef std::shared_ptr<Variable> VarPtr;


struct Expr : CliteNode {
	Expr() : CliteNode("expr") {}
};
struct Stmt : CliteNode {
	Stmt(const std::string& dbg_descr) : CliteNode("stmt '" + dbg_descr + "'") {}
};


typedef std::vector<ExprPtr> ExprList;



typedef std::shared_ptr<Stmt> StmtPtr;
typedef std::vector<StmtPtr> StmtList;


struct Block : CliteNode {
	StmtList statements;

	const std::string dbg_name;

	Block(const std::string& dbg_name)
	 : CliteNode("block '" + dbg_name + "'"), dbg_name(dbg_name) {}

	void move_nodes_into(Block* front, Block* back) {
		StmtList ns;

		if (front) {
			for (auto&& n : front->statements) {
				ns.push_back(std::move(n));
			}
		}

		for (auto&& n : statements) {
			ns.push_back(std::move(n));
		}

		if (back) {
			for (auto&& n : back->statements) {
				ns.push_back(std::move(n));
			}
		}

		statements = std::move(ns);
	}

};

typedef std::shared_ptr<Block> BlockPtr;

struct Function : Expr {
	std::string name;
	ExprList args;
	bool infix;

	Function(const std::string& name, const ExprList& args, bool infix = false);
};

struct Cast : Expr {
	std::string type;
	ExprPtr expr;

	Cast(const std::string& type, const ExprPtr& expr);
};

struct Reference : Expr {
	VarPtr var;

	Reference(const VarPtr& v);
};

struct ArrayAccess : Expr {
	ExprPtr array;
	ExprPtr index;

	ArrayAccess(const ExprPtr& array, const ExprPtr& index);
};

struct Literal : Expr {
	const std::string text;
	bool is_string;

	Literal(const std::string& val, bool str) : text(val), is_string(str) {}
};


struct Assign : Stmt {
	ExprPtr dest;
	ExprPtr expr;

	Assign(const ExprPtr& dest, const ExprPtr& e);
};

struct Effect : Stmt {
	ExprPtr expr;

	Effect(const ExprPtr& e) : Stmt("effect"), expr(e) {}
};

struct Scope : Stmt {
	StmtList statements;

	Scope(const StmtList& sts) : Stmt("scope"), statements(sts) {}
};

struct PredicatedStmt : Stmt {
	ExprPtr cond;
	StmtList then;

	PredicatedStmt(const ExprPtr& cond, const StmtList& then)
	 : Stmt("predicated"), cond(cond), then(then) {}
};

struct InlineTarget : Stmt {
	InlineTarget() : Stmt("inline_target") {}
};


enum BranchLikeliness {
	Never = -2,
	Unlikely = -1,
	Unknown = 0,
	Likely = 1,
	Always = 2,
};

enum BranchThreading {
	Irrelevant = 0,

	MustYield = 1,
	NeverYield = -1
};

struct Branch : Stmt {
	ExprPtr cond;
	Block* dest;

	BranchLikeliness likely;
	BranchThreading threading;
	bool is_exit = false;

	Branch(const ExprPtr& cond, Block* dest, BranchLikeliness likely, BranchThreading threading, bool is_exit)
	 : Stmt("branch"), cond(cond), dest(dest), likely(likely), threading(threading), is_exit(is_exit) {}
};

struct CommentStmt : Stmt {
	const std::string text;

	CommentStmt(const std::string& t) : Stmt("comment"), text(t) {}
};

struct PlainStmt : Stmt {
	const std::string text;

	PlainStmt(const std::string& t) : Stmt("plain '" + t + "'"), text(t) {}
};

struct Factory {
	StmtPtr assign(const ExprPtr& dest, const ExprPtr& expr) {
		return std::make_shared<Assign>(dest, expr);
	}

	StmtPtr assign(const VarPtr& dest, const ExprPtr& expr) {
		return assign(reference(dest), expr);
	}

	StmtPtr effect(const ExprPtr& expr) {
		return std::make_shared<Effect>(expr);
	}

	StmtPtr comment(const std::string& t) {
		return std::make_shared<CommentStmt>(t);
	}

	StmtPtr scope(const StmtList& s) {
		return std::make_shared<Scope>(s);
	}

	StmtPtr inline_target() {
		return std::make_shared<InlineTarget>();
	}

	StmtPtr predicated(const ExprPtr& cond, const StmtPtr& then) {
		return std::make_shared<PredicatedStmt>(cond, StmtList { then });
	}

	StmtPtr predicated(const ExprPtr& cond, const StmtList& then) {
		return std::make_shared<PredicatedStmt>(cond, then);
	}

	StmtPtr cond_branch(const ExprPtr& cond, Block* if_true,
			BranchLikeliness likely, BranchThreading threading = BranchThreading::Irrelevant) {
		return std::make_shared<Branch>(cond, if_true, likely, threading, false);
	}

	StmtPtr cond_branch(const ExprPtr& cond, const BlockPtr& if_true,
			BranchLikeliness likely, BranchThreading threading = BranchThreading::Irrelevant) {
		return cond_branch(cond, if_true.get(), likely, threading);
	}

	StmtPtr uncond_branch(Block* b,
			BranchLikeliness likely, BranchThreading threading = BranchThreading::Irrelevant) {
		return std::make_shared<Branch>(nullptr, b, likely, threading, false);
	}

	StmtPtr uncond_branch(const BlockPtr& b,
			BranchLikeliness likely, BranchThreading threading = BranchThreading::Irrelevant) {
		return uncond_branch(b.get(), likely, threading);
	}

	StmtPtr exit(BranchLikeliness likely = BranchLikeliness::Unlikely,
			BranchThreading threading = BranchThreading::Irrelevant) {
		return std::make_shared<Branch>(nullptr, nullptr, likely, threading, true);
	}

	StmtPtr plain(const std::string& p) {
		return std::make_shared<PlainStmt>(p);
	}

	StmtPtr __log(const std::string& name, const std::string& txt, const ExprList& params) {
		ExprList args;
		args.push_back(str_literal("LWT %d@%d: " + txt+ "\\n"));
		args.push_back(literal_from_str("schedule_idx"));
		args.push_back(literal_from_str("__LINE__"));
		for (auto& p : params) {
			args.push_back(p);
		}
		return effect(function(name, std::move(args)));
	}

	StmtPtr log_trace(const std::string& txt, const ExprList& params) {
		return __log("LOG_TRACE", txt, params);
	}

	StmtPtr log_debug(const std::string& txt, const ExprList& params) {
		return __log("LOG_DEBUG", txt, params);
	}

	StmtPtr log_error(const std::string& txt, const ExprList& params) {
		return __log("LOG_ERROR", txt, params);
	}

	ExprPtr cast(const std::string& name, const ExprPtr& a0) {
		return std::make_shared<Cast>(name, a0);
	}

	ExprPtr method(const VarPtr& object, const std::string& name, const ExprList& args);


	ExprPtr function(const std::string& name, const ExprList& args);

	ExprPtr function(const std::string& name, const ExprPtr& a0) {
		return function(name, ExprList { a0 });
	}
	ExprPtr function(const std::string& name, const ExprPtr& a0, const ExprPtr& a1) {
		return function(name, ExprList { a0, a1 });
	}
	ExprPtr function(const std::string& name, const ExprPtr& a0, const ExprPtr& a1, const ExprPtr& a2) {
		return function(name, ExprList { a0, a1, a2 } );
	}


	ExprPtr reference(const VarPtr& var) {
		return std::make_shared<Reference>(var);
	}

	template<typename T>
	ExprPtr literal_from_int(const T& v) {
		return std::make_shared<Literal>(std::to_string(v), false);
	}

	ExprPtr literal_from_str(const std::string& v) {
		return std::make_shared<Literal>(v, false);
	}

	ExprPtr str_literal(const std::string& v) {
		return std::make_shared<Literal>(v, true);
	}

	ExprPtr array_access(const ExprPtr& arr, const ExprPtr& idx) {
		return std::make_shared<ArrayAccess>(arr, idx);
	}

};

template<typename T>
struct Builder : Factory {
	T& m_current_block;

	Builder& operator<<(const StmtPtr& s) {
		m_current_block.statements.push_back(s);
		return *this;
	}

	template<typename X>
	Builder& operator<<(const X& ls) {
		for (auto& s : ls) {
			*this << s;
		}
		return *this;
	}

	Builder(T* b) : m_current_block(*b) {}
	Builder(T& b) : m_current_block(b) {}
};


struct Pass {
	void recurse_stmt(const StmtPtr& stmt);
	virtual void on_stmt(const StmtPtr& stmt);

	void recurse_expr(const ExprPtr& expr);
	virtual void on_expr(const ExprPtr& expr);
};

struct Fragment {
	std::vector<BlockPtr> blocks;

	std::string dbg_name;

private:
	std::unordered_map<std::string, VarPtr> var_names;

public:
	bool var_exists(const std::string& name) const {
		return var_names.find(name) != var_names.end();
	}

	VarPtr try_new_var(bool& created, const std::string& name, const std::string& type, Variable::Scope scope,
			bool constant = false, const ExprPtr& default_value = nullptr, const std::string& dbg_name = "") {
		if (var_exists(name)) {
			created = false;
			return var_names[name];
		}

		created = true;
		return std::make_shared<Variable>(name, type, scope, constant, default_value, dbg_name);
	}

	VarPtr new_var(const std::string& name, const std::string& type, Variable::Scope scope,
			bool constant = false, const std::string& default_value = "", const std::string& dbg_name = "");

	VarPtr new_outside_var(const std::string& name, const std::string& type) {
		VarPtr v = new_var(name, type, Variable::Scope::ThreadWide);
		v->outside = true;
		return v;
	}

	VarPtr clone_var(const std::string& name, const VarPtr& var);

	BlockPtr new_block(const std::string& dbg_name) {
		BlockPtr p = std::make_shared<Block>(dbg_name);
		blocks.push_back(p);
		return p;
	}

	void clear() {
		blocks.clear();
		var_names.clear();
	}
};

struct CGen {
	std::ostringstream decl;
	std::ostringstream impl;
	const std::string unique_prefix;
	const std::string local_state_var = "local_state";
	const std::string fsm_state_var = "fsm_state";

	enum Level {
		LocalVar = -1,
		LocalState = 0,
		ThreadWide = 1
	};

	std::unordered_map<std::string, Level> generated_variables;

private:
	struct StmtContext {
		std::ostringstream& impl;

		StmtContext(std::ostringstream& impl) : impl(impl) {}
	};

	void on_stmt(StmtContext& ctx, const StmtPtr& stmt);

	std::string on_expr(StmtContext& ctx, const ExprPtr& expr);

	std::unordered_map<Block*, std::string> block_mapping;
	std::unordered_map<Block*, std::unordered_set<VarPtr>> block_vars;

protected:
	void gen_jump(Block* b, BranchThreading threading);
	void gen_jump_exit(BranchThreading threading);

	void declare_var(std::ostringstream& out, const VarPtr& ptr,
		StmtContext& ctx, Level level, const std::string& dbg_cat);
	std::string get_var_id(const VarPtr& v) const;

	std::string fsm_set_state(const std::string& new_state) const;
	std::string fsm_get_state() const;
public:
	const size_t num_parallel;

	struct GenCtx {
		std::ostringstream& decl;
		std::ostringstream& impl;
	};
	void generate(Fragment& fragm, GenCtx& ctx);

	static size_t prune_states(Fragment& gen, size_t round,
		std::unordered_map<Block*, size_t>* copy_inline_count);

	CGen(size_t num_par, const std::string& unique_prefix)
	 : unique_prefix(unique_prefix), num_parallel(num_par) {}

};
} // clite

#endif