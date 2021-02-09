#include "flatten_statements_pass.hpp"
#include "runtime.hpp"

void
FlattenStatements::on_stmts(std::vector<StmtPtr>& statements)
{
	for (auto& s : statements) {
		on_stmts(s->statements);
	}


	// inline child (warp) statements here
	std::vector<StmtPtr> res;
	for (auto& s : statements) {
		ASSERT(s);
		if (s->type == Statement::Type::Wrap) {
			for (auto& r : s->statements) {
				ASSERT(r);
				res.push_back(r);
			}
		} else {
			res.push_back(s);
		}
	}

	statements = res;
}

void FlattenStatements::operator()(std::vector<StmtPtr>& statements)
{
	on_stmts(statements);
}

static void flatten(std::vector<StmtPtr>& statements)
{
	FlattenStatements f;
	f(statements);
}

void
FlattenStatementsPass::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	flatten(s->statements);

	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);
}

void
FlattenStatementsPass::on_lolepop(Pipeline& p, Lolepop& l)
{
	flatten(l.statements);

	recurse_lolepop(p, l);
}