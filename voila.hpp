#ifndef H_VOILA
#define H_VOILA

#include <string>
#include <vector>
#include <unordered_map>
#include <cmath>
#include <memory>

#include "blend_space_point.hpp"

// VOILA

struct ScalarTypeProps {
	double dmin = std::nan("0");
	double dmax = std::nan("1");
	std::string type = "";

	bool operator==(const ScalarTypeProps& o) const {
		return dmin == o.dmin && dmax == o.dmax && type == o.type;
	}

	bool operator!=(const ScalarTypeProps& other) const {
		return !operator==(other);
	}

	void write(std::ostream& o, const std::string& prefix = "") const;

	void from_cover(ScalarTypeProps& a, ScalarTypeProps& b);
};

struct TypeProps {
	enum Category {
		Unknown = 0,
		Tuple,
		Predicate
	};
	Category category = Unknown;
	std::vector<ScalarTypeProps> arity;

	bool operator==(const TypeProps& other) const {
		return category == other.category && arity == other.arity;
	}

	bool operator!=(const TypeProps& other) const {
		return !operator==(other);
	}

	void write(std::ostream& o, const std::string& prefix = "") const;

	void from_cover(TypeProps& a, TypeProps& b);
};

#include <unordered_set>

struct Expression;
struct Statement;

struct BlendConfig;

typedef std::shared_ptr<BlendConfig> BlendConfigPtr;

struct LoopReferencesProp {
	std::unordered_set<std::string> ref_strings;

	void insert(const std::string& s);
};

typedef std::shared_ptr<Expression> ExprPtr;

struct Properties {
	TypeProps type;

	bool scalar = false;
	bool constant = false;
	bool refers_to_variable = false; // set in LimitVariableLifetimeCheck

	std::string first_touch_random_access; // !empty when doing prefetch, also tells column to prefetch

	std::vector<ExprPtr> forw_affected_scans; // for scan_pos, later scanned columns

	std::vector<bool> gen_recurse;

	LoopReferencesProp loop_refs;

	std::shared_ptr<Expression> vec_cardinality;
};

struct Node {
	enum Type {
		ExprNode, StmtNode
	};
	const Type node_type;
	Properties props;

	Node(Type t) : node_type(t) {
	}

	std::string dbg_descr() const;

	virtual std::string type2string() const = 0;
	bool is_expr() const {
		return node_type == ExprNode;
	}

	Expression* as_expression() {
		if (!is_expr()) {
			return nullptr;
		}

		return (Expression*)this;
	}

	bool is_stmt() const {
		return node_type == StmtNode;
	}

	Statement* as_statement() {
		if (!is_stmt()) {
			return nullptr;
		}

		return (Statement*)this;
	}

	BlendConfigPtr blend_op_get_config() const;

	bool is_blend_stmt() const {
		return get_blend_op_type() == -1;
	}

	bool is_blend_op() const {
		return is_blend_stmt();
	}

private:
	int get_blend_op_type() const;
};

typedef std::shared_ptr<Node> NodePtr;

struct Expression : Node {
	enum Type {
		Function, Constant, Reference, LoleArg, LolePred, NoPred
	};

	const Type type;
	std::string fun;
	std::vector<std::shared_ptr<Expression>> args;
	std::shared_ptr<Expression> pred;

	bool is_get_pos() const;
	bool is_get_morsel() const;

	bool is_aggr(bool* out_global = nullptr) const;
	bool is_table_op(bool* table_out = nullptr,
		bool* table_in = nullptr, bool* col = nullptr) const;

	bool is_constant() const {
		return type == Constant;
	}

	bool is_cast() const;
	bool is_select() const;
	size_t get_table_column_ref(std::string& tbl_col) const; 
	size_t get_table_column_ref(std::string& tbl, std::string& col) const; 
	size_t get_table_ref(std::string& tbl) const; 
	bool is_terminal() const;
	bool is_tupleop() const;

	bool has_result() const;

	std::string type2string() const override;

	std::shared_ptr<Expression> clone() const;


	Expression(Type type, const std::string& fun,
		const std::shared_ptr<Expression>& pred = nullptr)
	 : Node(Node::Type::ExprNode), type(type), fun(fun), pred(pred) {}

	Expression(Type type, const std::string& fun,
		const std::vector<std::shared_ptr<Expression>>& args,
		const std::shared_ptr<Expression>& pred = nullptr)
		 : Node(Node::Type::ExprNode), type(type), fun(fun), args(args), pred(pred) {}
};


typedef std::vector<std::shared_ptr<Expression>> ExprList;


struct Statement : Node {
	enum Type {
		Loop, Assignment, EffectExpr, Emit, Done, Wrap, BlendStmt, MetaStmt
	};

	const Type type;
	std::string var;
	ExprPtr expr;
	ExprPtr pred;
	std::vector<std::shared_ptr<Statement>> statements;


	std::string type2string() const override;
protected:
	Statement(Type t, const std::string var = "",
		const ExprPtr& expr = nullptr,
		const ExprPtr& pred = nullptr)
	 : Node(Node::Type::StmtNode), type(t), var(var), expr(expr), pred(pred) {
	}

	Statement(Type t, const std::vector<std::shared_ptr<Statement>>& s,
		const std::string var = "",
		const ExprPtr& expr = nullptr,
		const ExprPtr& pred = nullptr)
	 : Node(Node::Type::StmtNode), type(t), var(var), expr(expr), pred(pred), statements(s) {}
};


typedef std::shared_ptr<Statement> StmtPtr;
typedef std::vector<StmtPtr> StmtList;

struct Lolepop {
	const std::string name;
	std::vector<std::shared_ptr<Statement>> statements;	

	Lolepop(const std::string& name,
		const std::vector<std::shared_ptr<Statement>>& statements)
	 : name(name), statements(statements) {}
};

struct Pipeline {
	std::vector<std::shared_ptr<Lolepop>> lolepops;

	bool tag_interesting = true; //!< To focus exploration

	// Pipeline(std::vector<Lolepop*>& lolepops) : lolepops(lolepops) {}
};

struct DCol {
	enum Modifier {
		kValue,
		kKey,
		kHash
	};

	const std::string name;
	const std::string source; // only for columns of BaseTables
	const Modifier mod;

	DCol(const std::string& name, const std::string& source = "", Modifier mod = Modifier::kValue)
		: name(name), source(source), mod(mod){

	}
};

struct DataStructure {
	enum Type {
		kTable,
		kHashTable,
		kBaseTable
	};

	typedef int Flags;
	static constexpr Flags kThreadLocal = 1 << 1;
	static constexpr Flags kReadAfterWrite = 1 << 2;
	static constexpr Flags kFlushToMaster = 1 << 3;
	static constexpr Flags kDefault = 0;

	static std::string type_to_str(Type t);

	const std::string name;
	const std::string source; // only for BaseTables
	const Type type;
	const Flags flags;

	const std::vector<DCol> cols;

	DataStructure(const std::string& name, const Type& type, const Flags& flags,
		const std::vector<DCol>& cols, const std::string& source = "")
		: name(name), source(source), type(type), flags(flags), cols(cols) {
	}
};

struct Program {
	std::vector<Pipeline> pipelines;
	std::vector<DataStructure> data_structures;

	// Program(std::vector<Pipeline>& pipelines) : pipelines(pipelines) {}
};


struct CrossingVariables {
	// all inputs going across this statement
	std::unordered_set<std::string> all_inputs;
	// all outputs going across this statement
	std::unordered_set<std::string> all_outputs;

	// used inside the statement
	std::unordered_set<std::string> used_inputs;

	// generated/update inside the statement
	std::unordered_set<std::string> used_outputs;
};

// Syntactic sugar
struct Loop : Statement {
	Loop(const ExprPtr& pred,
		const std::vector<std::shared_ptr<Statement>>& stms)
	 : Statement(Statement::Type::Loop, stms, "", pred) {}

	CrossingVariables crossing_variables;
};

struct WrapStatements : Statement {
	WrapStatements(const std::vector<std::shared_ptr<Statement>>& stms,
		const ExprPtr& pred)
	 : Statement(Statement::Type::Wrap, stms, "", pred) {}
};


struct Assign : Statement {
	Assign(const std::string& dest,
		const ExprPtr& expr,
		const ExprPtr& pred)
	 : Statement(Statement::Type::Assignment, dest, expr, pred) {}
};

struct Effect : Statement {
	Effect(const ExprPtr& expr)
	 : Statement(Statement::Type::EffectExpr, "", expr) {}
};

struct Emit : Statement {
	Emit(const ExprPtr& expr,
		const ExprPtr& pred);
};

struct Done : Statement {
	Done(): Statement(Statement::Type::Done, "") {}
};


struct LolePred : Expression {
	LolePred() : Expression(Expression::Type::LolePred, "") {}
};

struct Const : Expression {
	Const(const std::string& val) : Expression(Expression::Type::Constant, val) {}
	Const(int val) : Expression(Expression::Type::Constant, std::to_string(val)) {}
};

struct LoleArg : Expression {
	LoleArg() : Expression(Expression::Type::LoleArg, "") {}
};

struct Fun : Expression {
	Fun(const std::string& fun,
		const std::vector<ExprPtr>& exprs,
		const ExprPtr& pred)
	 : Expression(Expression::Type::Function, fun, exprs, pred) {}

	virtual ~Fun() {}
};

struct TupleAppend : Fun {
	TupleAppend(const std::vector<ExprPtr>& expr,
		const ExprPtr& pred) : Fun("tappend", expr, pred) {}
};

struct BlendStmt : Statement {
	BlendStmt(const StmtList& stms, const ExprPtr& pred)
	 : Statement(Statement::Type::BlendStmt, stms, "", pred) {}

	BlendConfigPtr blend;

	CrossingVariables crossing_variables;

	ExprPtr get_predicate() const;
};

struct MetaStmt : Statement {
	enum MetaType {
		VarDead,
		RefillInflow,
		FsmExclusive,
	};

	const MetaType meta_type;

	MetaStmt(MetaType t)
	 : Statement(Statement::Type::MetaStmt, {}, "", nullptr), meta_type(t) {
	}
};

struct MetaVarDead : MetaStmt {
	std::string variable_name;
	MetaVarDead(const std::string& var) : MetaStmt(MetaStmt::MetaType::VarDead),
		variable_name(var) {

	}
};

struct MetaFsmExclusive : MetaStmt {
	const bool begin;

	MetaFsmExclusive(bool begin)
	 : MetaStmt(MetaStmt::MetaType::FsmExclusive), begin(begin)
	{}
};

struct MetaBeginFsmExclusive : MetaFsmExclusive {
	MetaBeginFsmExclusive() : MetaFsmExclusive(true) {}
};

struct MetaEndFsmExclusive : MetaFsmExclusive {
	MetaEndFsmExclusive() : MetaFsmExclusive(false) {}
};

struct MetaRefillInflow : MetaStmt {
	MetaRefillInflow() : MetaStmt(MetaStmt::MetaType::RefillInflow) {}
};

struct TupleGet : Fun {
	static long long get_idx(Expression& e);

	TupleGet(const ExprPtr& e, size_t idx);
};

struct Scan : Fun {
	Scan(const ExprPtr& col,
		const ExprPtr& pos,
		const ExprPtr& pred)
	 : Fun("scan", {col, pos}, pred) { }
};

struct Print : Fun {
	Print(const ExprPtr& col,
		const ExprPtr& pred)
	 : Fun("print", {col}, pred) { }	
};

struct Scatter : Effect {
	Scatter(const ExprPtr& col,
		const ExprPtr& idx,
		const ExprPtr& val,
		const ExprPtr& pred)
	 : Effect(std::make_shared<Fun>("scatter", ExprList {col, idx, val}, pred)) { }	
};

struct Write : Effect {
	Write(const ExprPtr& col,
		const ExprPtr& wpos,
		const ExprPtr& val,
		const ExprPtr& pred)
	 : Effect(std::make_shared<Fun>("write", ExprList {col, wpos, val}, pred)) {}
};


struct Aggr2 : Effect {
	Aggr2(const ExprPtr& col,
		const ExprPtr& idx,
		const ExprPtr& val,
		const std::string& name,
		const ExprPtr& pred)
	 : Effect(std::make_shared<Fun>(name, ExprList {col, idx, val}, pred)) { }	
};

#define _Aggr2(external, internal) struct external : Aggr2 { \
		external(const ExprPtr& a, \
				const ExprPtr& b, \
				const ExprPtr& c, \
				const ExprPtr& p) \
		: Aggr2(a, b, c, internal, p) {} \
	};
_Aggr2(AggrCount, "aggr_count")
_Aggr2(AggrSum, "aggr_sum")
_Aggr2(AggrMin, "aggr_min")
_Aggr2(AggrMax, "aggr_max")

struct AggrGSum : Effect {
	AggrGSum(const ExprPtr& col,
		const ExprPtr& val,
		const ExprPtr& pred)
	: Effect(std::make_shared<Fun>("aggr_gsum", ExprList {col, val}, pred)) { }	
};

struct AggrGCount : Effect {
	AggrGCount(const ExprPtr& col,
		const ExprPtr& pred)
	: Effect(std::make_shared<Fun>("aggr_gcount", ExprList {col}, pred)) { }	
};

struct Ref : Expression {
	Ref(const std::string& var) : Expression(Expression::Type::Reference, var) {}
};

struct Table : DataStructure {
	Table(const std::string& name, const std::vector<DCol>& cols,
		const DataStructure::Type& type = DataStructure::kTable,
		const DataStructure::Flags& flags = DataStructure::kDefault);
};

struct HashTable : Table {
	HashTable(const std::string& name, const DataStructure::Flags& flags, const std::vector<DCol>& cols)
	 : Table(name, cols, DataStructure::kHashTable, flags) {}
};

struct BaseTable : DataStructure {
	BaseTable(const std::string& name, const std::vector<DCol>& cols, const std::string& source)
	 : DataStructure(name, DataStructure::kBaseTable, DataStructure::kDefault, cols, source) {}
};

#endif