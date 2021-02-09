#ifndef H_FLATTEN_STATEMENTS_PASS
#define H_FLATTEN_STATEMENTS_PASS

#include "pass.hpp"

struct FlattenStatements {
private:
	void on_stmts(std::vector<StmtPtr>& statements);
public:
	void operator()(std::vector<StmtPtr>& statements);
};

struct FlattenStatementsPass : Pass {
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_lolepop(Pipeline& p, Lolepop& l) override;
};


#endif