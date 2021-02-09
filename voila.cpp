#include "voila.hpp"
#include "runtime.hpp"
#include "utils.hpp"

std::string
Node::dbg_descr() const
{
	switch (node_type) {
	case ExprNode:
		{
			auto s = (Expression*)this;
			return("expression " + s->type2string() + " '" + s->fun + "'");
		}
		break;
	case StmtNode:
		{
			auto s = (Statement*)this;
			return("statement " + s->type2string() + " '" + s->var + "'");
		}
		break;
	default:
		ASSERT(false);
		return "";
	}
}

int
Node::get_blend_op_type() const
{
	if (is_stmt()) {
		auto stmt = (Statement*)(this);
		if (stmt && stmt->type == Statement::Type::BlendStmt) {
			return -1;
		}		
	}

	return 0;
}

BlendConfigPtr
Node::blend_op_get_config() const
{
	switch (get_blend_op_type()) {
	case -1:
		{
			auto& stmt = *(BlendStmt*)(this);
			return stmt.blend;
		}
	default:
		return nullptr;
	}
}

void
ScalarTypeProps::write(std::ostream& o, const std::string& prefix) const
{
	o
		<< prefix << "ScalarTypeProps {" << std::endl
		<< prefix << "  dmin=" << dmin << std::endl
		<< prefix << "  dmax=" << dmax << std::endl
		<< prefix << "  type=" << type << std::endl
		<< prefix << "}" << std::endl;
}

void
ScalarTypeProps::from_cover(ScalarTypeProps& a, ScalarTypeProps& b)
{
	ASSERT(a.type == b.type);
	type = a.type;

	dmin = std::min(a.dmin, b.dmin);
	dmax = std::max(a.dmax, b.dmax);
}

void
TypeProps::write(std::ostream& o, const std::string& prefix) const
{
	o << prefix << "TypeProps {" << std::endl;
	for (auto& t : arity) {
		t.write(o, prefix + " ");
	}
	o << prefix << "}" << std::endl;
}

void
TypeProps::from_cover(TypeProps& a, TypeProps& b)
{
	ASSERT(a.arity.size() == b.arity.size());

	arity = a.arity;

	for (size_t i=0; i<arity.size(); i++) {
		arity[i].from_cover(a.arity[i], b.arity[i]);
	}

	ASSERT(a.category == b.category);
	category = a.category;
}

void
LoopReferencesProp::insert(const std::string& s)
{
	ref_strings.insert(s);
}

long long
TupleGet::get_idx(Expression& e)
{
	auto& args = e.args;
	ASSERT(args.size() == 2 && "must be binary");
	ASSERT(args[1]->props.type.arity.size() == 1 &&
		args[1]->type == Expression::Type::Constant &&
		"Tuple index must be simple scalar Constant");
	
	long long idx = -1;
	bool parsed = parse_cardinal(idx, args[1]->fun);

	ASSERT(parsed && idx >= 0 && "Constant value must be integer >= 0");
	return idx;
}

bool
Expression::is_get_pos() const
{
	if (type != Type::Function) {
		return false;
	}
	auto& n = fun;
	if (!n.compare("scan_pos") || !n.compare("read_pos") ||
			!n.compare("write_pos")) {
		return true;
	}
	return false;
}

bool
Expression::is_get_morsel() const
{
	if (type != Type::Function) {
		return false;
	}
	auto& n = fun;
	if (!n.compare("scan_morsel") || !n.compare("read_morsel") ||
			!n.compare("write_morsel")) {
		return true;
	}
	return false;

}

bool
Expression::is_aggr(bool* out_global) const
{
	if (out_global) {
		*out_global = false;
	}

	if (type != Type::Function) {
		return false;
	}

	bool global = false;
	auto& n = fun;

	global = str_in_strings(n, {"aggr_gcount", "aggr_gsum", "aggr_gconst1"});
	if (global || str_in_strings(n, {"aggr_count", "aggr_sum", "aggr_min", "aggr_max"})) {
		if (out_global) {
			*out_global = global;
		} 
		return true;
	}

	return false;
}

bool
Expression::is_table_op(bool* table_out, bool* table_in, bool* col) const
{
	if (table_in)	*table_in = false;
	if (table_out)	*table_out = false;
	if (col)		*col = false;

	if (type != Type::Function) {
		return false;
	}
	const auto& n = fun;

	if (is_aggr()) {
		if (table_in) 	*table_in = true;
		if (table_out) 	*table_out = true;
		if (col)		*col = true;
		return true;
	}

	if (!n.compare("bucket_insert") || !n.compare("bucket_link") ||
			!n.compare("bucket_build") || !n.compare("bucket_flush")) {
		if (table_in) 	*table_in = true;
		if (table_out) 	*table_out = true;
		return true;
	}

	
	if (!n.compare("read") || !n.compare("gather") || !n.compare("check")) {
		if (table_in)	*table_in = true;
		if (col)		*col = true;
		return true;
	}

	if (!n.compare("bucket_lookup") || !n.compare("bucket_next")) {
		if (table_in)	*table_in = true;
		if (col)		*col = false;
		return true;
	}

	if (!n.compare("write") || !n.compare("scatter")) {
		if (table_out)	*table_out = true;
		if (col)		*col = true;
		return true;
	}

	return false;
}

bool
Expression::is_cast() const
{
	if (type != Type::Function) {
		return false;
	}

	return type_code_from_str(fun.c_str()) != TypeCode_invalid;
}

bool
Expression::is_select() const
{
	if (type != Type::Function) {
		return false;
	}
	auto& n = fun;
	if (!n.compare("selvalid") || !n.compare("seltrue") ||
		!n.compare("selfalse") || !n.compare("selunion")) {
		return true;
	}
	
	return false;
}

size_t
Expression::get_table_column_ref(std::string& tbl_col) const
{
	if (type != Type::Function) {
		return 0;
	}
	auto& n = fun;
	bool column = false;
	if (is_table_op(nullptr, nullptr, &column)) {
		tbl_col = args[0]->fun;

		return column ? 2 : 1;
	}

	if (!n.compare("scan")) {
		tbl_col = args[0]->fun;
		return 2;		
	}

	if (is_get_pos()) {
		tbl_col = args[0]->fun;
		return 1;
	}
	
	return 0;
}

size_t
Expression::get_table_column_ref(std::string& tbl, std::string& col) const
{
	if (type != Type::Function) {
		return 0;
	}

	size_t r = get_table_column_ref(tbl);

	if (r == 2) {
		auto ss = split(tbl, '.');
		ASSERT(ss.size() == 2);

		tbl = ss[0];
		col = ss[1];
		return r;
	}

	if (r == 1) {
		tbl = tbl;
		return r;
	}

	return 0;
}

size_t
Expression::get_table_ref(std::string& tbl) const
{
	size_t r = get_table_column_ref(tbl);
	ASSERT(r == 1 || r == 2);
	return r;
}

bool
Expression::is_terminal() const
{
	switch (type) {
		case Function:
			return false;
		case Constant:
		case Reference:
		case LoleArg:
		case LolePred:
		case NoPred:
			return true;

		default:
			ASSERT(false);
			return false;
	}
}

bool
Expression::is_tupleop() const
{
	const auto& n = fun;

	switch (type) {
	case Function:
		if (!n.compare("tget") || !n.compare("tappend")) {
			return true;
		}
		return false;
	default:
		return false;
	}
}

bool
Expression::has_result() const
{
	if (is_aggr()) {
		return false;
	}

	const auto& n = fun;

	if (!n.compare("write") || !n.compare("scatter")) {
		return false;
	}

	if (!n.compare("bucket_link") || !n.compare("bucket_build")) {
		return false;
	}

	return true;
}

std::string
Expression::type2string() const
{
	switch (type) {
#define A(N) case N: return std::string(#N)
		A(Function);
		A(Constant);
		A(Reference);
		A(LoleArg);
		A(LolePred);
		A(NoPred);

		default:
			ASSERT(false);
			return "";
	}
#undef A
}

ExprPtr
Expression::clone() const
{
	ExprPtr r = std::make_shared<Expression>(type, fun, args, pred);
	r->props = props;
	return r;
}

std::string
Statement::type2string() const
{
	switch (type) {
	case Statement::Type::Loop: return "Loop";
	case Statement::Type::Assignment: return "Assignment";
	case Statement::Type::EffectExpr: return "EffectExpr";
	case Statement::Type::Emit: return "Emit";
	case Statement::Type::Done: return "Done";
	case Statement::Type::Wrap: return "Wrap";
	case Statement::Type::BlendStmt: return "BlendStmt";
	default:	return "???";
	}
}


Emit::Emit(const std::shared_ptr<Expression>& expr,
	const std::shared_ptr<Expression>& pred)
: Statement(Statement::Type::Emit, "", expr, pred)
{
	ASSERT(expr);
}

TupleGet::TupleGet(const std::shared_ptr<Expression>& e, size_t idx)
 : Fun("tget", {e, std::make_shared<Const>(std::to_string(idx))}, nullptr)
{ // , new Constant(std::to_string(idx))
	ASSERT(e);
} 

std::string
DataStructure::type_to_str(Type t)
{
	switch (t) {
	case kTable:	return "Table";
	case kHashTable:return "HashTable";
	case kBaseTable:return "BaseTable";
	default:		ASSERT(false); return "";
	}
}

Table::Table(const std::string& name, const std::vector<DCol>& cols,
	const DataStructure::Type& type, const DataStructure::Flags& flags)
 : DataStructure(name, type, flags, cols)
{
	ASSERT(type == DataStructure::kTable || type == DataStructure::kHashTable);
}


ExprPtr
BlendStmt::get_predicate() const
{
	ASSERT(expr);
	return expr;
}