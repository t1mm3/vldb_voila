#ifndef H_RELALG
#define H_RELALG

#include <string>
#include <vector>
#include <memory>

namespace relalg {

struct RelExprVisitor;

struct RelExpr {
	static std::shared_ptr<RelExpr>
	from_column_name(const std::string& name);

	static std::vector<std::shared_ptr<RelExpr>>
	from_column_names(const std::vector<std::string>& names);

	enum Type {
		Const, ColId, Fun, Assign
	};

	const Type type;
	RelExpr(Type t) : type(t) {}
	virtual ~RelExpr() {}

	virtual void accept(RelExprVisitor& visitor) = 0;
};

struct Const : RelExpr {
	const std::string val;
	Const(const std::string& val);
	Const(int64_t v);

	void accept(RelExprVisitor& visitor) final;
};

struct ColId : RelExpr {
	const std::string id;

	ColId(const std::string& id) : RelExpr(RelExpr::Type::ColId), id(id) {}

	void accept(RelExprVisitor& visitor) final;
};

struct Fun : RelExpr {
	const std::string name;
	const std::vector<std::shared_ptr<RelExpr>> args;

	Fun(const std::string& id, const std::vector<std::shared_ptr<RelExpr>>& args)
	 : RelExpr(RelExpr::Type::Fun), name(id), args(args) {}

	void accept(RelExprVisitor& visitor) final;

	static std::shared_ptr<RelExpr>
	create_left_deep_tree(const std::string& name,
		const std::vector<std::shared_ptr<RelExpr>>& exprs);
};

struct Assign : RelExpr {
	const std::string name;
	std::shared_ptr<RelExpr> expr;

	Assign(const std::string& id, std::shared_ptr<RelExpr>&& e)
	 : RelExpr(RelExpr::Type::Assign), name(id), expr(e) {}

	void accept(RelExprVisitor& visitor) final;
};

struct RelExprVisitor {
	virtual void visit(Const&) = 0;
	virtual void visit(ColId&) = 0;
	virtual void visit(Fun&) = 0;
	virtual void visit(Assign&) = 0;
};

struct RelOpVisitor;

struct RelOp {
	const std::string name;

	std::shared_ptr<RelOp> left;
	std::shared_ptr<RelOp> right;

	virtual void accept(RelOpVisitor& visitor) = 0;

protected:
	RelOp(const std::string& name, const std::shared_ptr<RelOp>& left = nullptr,
		const std::shared_ptr<RelOp>& right = nullptr)
	 : name(name), left(left), right(right) {}
};

struct Scan : RelOp {
	const std::vector<std::shared_ptr<RelExpr>> columns;
	const std::string table;

	Scan(const std::string& table, const std::vector<std::shared_ptr<RelExpr>>& columns)
	 : RelOp("Scan"), columns(columns), table(table) {}

	void accept(RelOpVisitor& visitor) override;
};

struct Project : RelOp {
	const std::vector<std::shared_ptr<RelExpr>> projections;

	Project(const std::shared_ptr<RelOp>& child, const std::vector<std::shared_ptr<RelExpr>>& projections)
	 : RelOp("Project", child), projections(projections) {}

	void accept(RelOpVisitor& visitor) override;
};

struct Select : RelOp {
	std::shared_ptr<RelExpr> predicate;

	Select(const std::shared_ptr<RelOp>& child, std::shared_ptr<RelExpr>&& predicate)
	 : RelOp("Select", child), predicate(predicate) {}

	void accept(RelOpVisitor& visitor) override;
};

struct HashAggr : RelOp {
	enum Variant {
		Hash,
		Global,
	};
	const Variant variant;
	const std::vector<std::shared_ptr<RelExpr>> keys;
	const std::vector<std::shared_ptr<RelExpr>> aggregates;

	HashAggr(Variant variant, const std::shared_ptr<RelOp>& child,
		const std::vector<std::shared_ptr<RelExpr>>& keys,
		const std::vector<std::shared_ptr<RelExpr>>& aggregates)
	 : RelOp("HashAggr", child), variant(variant), 
	 keys(keys), aggregates(aggregates)	{}

	void accept(RelOpVisitor& visitor) override;
};

struct HashJoin : RelOp {
	enum Variant {
		Join01,
		JoinN
	};

	const Variant variant;

	// PROBE
	const std::vector<std::shared_ptr<RelExpr>> left_keys;
	const std::vector<std::shared_ptr<RelExpr>> left_payl;

	// BUILD
	const std::vector<std::shared_ptr<RelExpr>> right_keys;
	const std::vector<std::shared_ptr<RelExpr>> right_payl;

	HashJoin(Variant variant,
		const std::shared_ptr<RelOp>& left,
		const std::vector<std::shared_ptr<RelExpr>>& left_keys,
		const std::vector<std::shared_ptr<RelExpr>>& left_payl,

		const std::shared_ptr<RelOp>& right,
		const std::vector<std::shared_ptr<RelExpr>>& right_keys,
		const std::vector<std::shared_ptr<RelExpr>>& right_payl)
	 : RelOp("HashJoin", left, right), variant(variant), left_keys(left_keys),
	 left_payl(left_payl), right_keys(right_keys), right_payl(right_payl) {}

	void accept(RelOpVisitor& visitor) override;
};

struct RelOpVisitor {
	virtual void visit(Scan&) = 0;
	virtual void visit(Project&) = 0;
	virtual void visit(Select&) = 0;
	virtual void visit(HashAggr&) = 0;
	virtual void visit(HashJoin&) = 0;
};

struct RootHolder {
	std::shared_ptr<RelOp> root;
};

}; // relalg

#endif