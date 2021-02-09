#include "relalg.hpp"
#include "runtime.hpp"
#include <functional>

using namespace relalg;

std::shared_ptr<RelExpr>
RelExpr::from_column_name(const std::string& name)
{
	return std::make_shared<relalg::ColId>(name);
}

std::vector<std::shared_ptr<RelExpr>>
RelExpr::from_column_names(const std::vector<std::string>& names)
{
	std::vector<std::shared_ptr<RelExpr>> r;
	std::transform(names.begin(), names.end(), std::back_inserter(r),
		[] (const auto& name) {
			return from_column_name(name);
	});
	return r;
}

#define VISIT_EXPR(op) void op::accept(RelExprVisitor& visitor) { visitor.visit(*this); }
#define VISIT_OP(op) void op::accept(RelOpVisitor& visitor) { visitor.visit(*this); }

VISIT_EXPR(Const)
VISIT_EXPR(ColId)
VISIT_EXPR(Fun)
VISIT_EXPR(Assign)

VISIT_OP(Scan)
VISIT_OP(Project)
VISIT_OP(Select)
VISIT_OP(HashAggr)
VISIT_OP(HashJoin)

Const::Const(const std::string& val)
 : RelExpr(RelExpr::Type::Const), val(val)
{

}

Const::Const(int64_t v)
 : RelExpr(RelExpr::Type::Const), val(std::to_string(v))
{
}

std::shared_ptr<RelExpr>
Fun::create_left_deep_tree(const std::string& name, const std::vector<std::shared_ptr<RelExpr>>& exprs)
{
	ASSERT(exprs.size() > 0);
	std::shared_ptr<RelExpr> r = exprs[0];

	for (size_t i=1; i<exprs.size(); i++) {
		r = std::make_shared<Fun>(name, std::vector<std::shared_ptr<RelExpr>> {r, exprs[i]});
	}

	return r;
}