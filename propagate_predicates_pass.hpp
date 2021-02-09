#ifndef H_PROPAGATE_PREDICATES_PASS
#define H_PROPAGATE_PREDICATES_PASS

#include "pass.hpp"

#include <set>
struct LoopReferences : Pass {
	std::vector<StmtPtr> loops;

	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;
};

struct DisambiguatePass : Pass {
	std::unordered_set<std::string> vars;

	void on_program(Program& p) override;
	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(IPass::LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(IPass::LolepopCtx& ctx, ExprPtr& s) override;
};


struct AnnotateVectorCardinalityPass : Pass {
	std::unordered_map<std::string, ExprPtr> vars;

	std::unordered_set<ExprPtr> traversed;

	void on_statement(IPass::LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(IPass::LolepopCtx& ctx, ExprPtr& s) override;

	bool trace = false;
};

struct UsedVariables : Pass {
	struct VarInfo {
		std::unordered_set<ExprPtr> reads;
		std::unordered_set<StmtPtr> writes;
	};

	std::unordered_map<std::string, VarInfo> inflight;

	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;

	void on_program(Program& p) override;
};

struct CaptureBlendCrossingVariables : Pass {
	struct VarInfo {
		std::unordered_set<ExprPtr> reads;
		std::unordered_set<StmtPtr> writes;
	};

	std::unordered_map<std::string, VarInfo> inflight;

	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;

	void on_program(Program& p) override;
};

struct LimitVariableLifetimeCheck : Pass {
	void on_program(Program& p) override;

	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;

	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;
	void on_lolepop(Pipeline& p, Lolepop& l) override;

	std::unordered_set<std::string> active_vars;
	std::unordered_set<std::string> all_vars;

	size_t num_fails = 0;
	bool trace;

};

struct FirstDataTouchPass : Pass {
	size_t op_random_touches = 0;
	std::vector<ExprPtr> op_scanned;
	std::vector<ExprPtr> op_scanned_attach_to;
	size_t op_scan_id = 0;

	FirstDataTouchPass();

	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;
};

#if 0
struct PropagatePredicates : Pass {
	struct Ref {
		Statement* stmt;
	};
	std::unordered_map<std::string, Ref> vars;

	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(IPass::LolepopCtx& ctx, Statement& s) override;
	void on_expression(IPass::LolepopCtx& ctx, Expression& s)  override;

	void on_done() override;

	void add_preds(Node& n, const std::vector<Expression*>& ps, const char* dbg);
};
#endif

#endif