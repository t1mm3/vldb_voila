#include "cg_fuji_data.hpp"
#include "cg_fuji.hpp"
#include "runtime.hpp"
#include "utils.hpp"
#include "blend_context.hpp"

#define CLITE_LOG(...) log_trace(__VA_ARGS__)

#define EOL std::endl
static const std::string kColPrefix = "col_";

DataGenExpr::DataGenExpr(DataGen& gen, const clite::VarPtr& v,
	bool predicate)
 : var(v), predicate(predicate),
 	src_data_gen(gen.get_flavor_name()), data_gen(gen)
{
}

void
DataGen::put(ExprPtr& e, DataGenExprPtr&& expr)
{
	ASSERT(expr);
	m_codegen.put(e, std::move(expr));
}

DataGenExprPtr
DataGen::get_ptr(const ExprPtr& e)
{
	return m_codegen.get_ptr(e);
}

std::string
DataGen::unique_id()
{
	return m_codegen.unique_id();
}


DataGenExprPtr
DataGen::gen_get_expr(ExprPtr& e)
{
	gen_expr(e);
	auto ptr = get_ptr(e);
	ASSERT(ptr);
	return ptr;
}

DataGenExprPtr
DataGen::gen_get_pred(ExprPtr& e)
{
	gen_pred(e);
	auto ptr = get_ptr(e);
	ASSERT(ptr);
	return ptr;
}

DataGen::DataGen(FujiCodegen& cg, const std::string& name)
  : m_codegen(cg), m_name(name)
{

}

DataGen::~DataGen()
{

}

DataGenExprPtr
DataGen::gen_emit(StmtPtr& e, Lolepop* op)
{
	DataGenExprPtr null;
	return null;
}

void
DataGen::gen_extra(clite::StmtList& stmts, ExprPtr& e)
{
}

void
DataGen::gen_stmt(StmtPtr& e) {
	switch (e->type) {
	case Statement::Type::EffectExpr:
		break;
	default:
		ASSERT(false && "Handled by other layer");
		break;
	}		
}

#include "runtime_framework.hpp"

DataGenBuffer::DataGenBuffer(const QueryConfig& qconf, const BlendConfig& in_cfg,
	const BlendConfig& out_cfg, const std::string& c_name, const std::string& dbg_name)
 : c_name(c_name), in_config(in_cfg), out_config(out_cfg), dbg_name(dbg_name) {
	buffer_size = 0;
	read_granularity = 0;
	write_granularity = 0;

	const size_t kMulThreshold = 2;
	const size_t kMulBufferSize = 32;

	auto on_config = [&] (const BlendConfig& c, bool input) {
		size_t tuples;

		size_t osize = 0;
		auto ct = parse_computation_type(osize, qconf, c.computation_type);

		switch (ct) {
		case BaseFlavor::Avx512:
			tuples = 8;
			break;

		case BaseFlavor::Scalar:
			tuples = 1;
			break;

		case BaseFlavor::Vector:
			tuples = osize;
			break;

		default:
			std::cerr << "Invalid flavor: computation_type '" <<
				c.computation_type << "'" << std::endl;
			exit(1); 
			break;
		}

		if (input) {
			read_granularity = tuples;
		} else {
			write_granularity = tuples;
		}

		if (tuples*kMulBufferSize > buffer_size) {
			buffer_size = tuples*kMulBufferSize;
		}

		if (tuples*kMulThreshold > high_water_mark) {
			high_water_mark = tuples*kMulThreshold;
		}
	};


	high_water_mark = kMulThreshold*1024;
	buffer_size = kMulBufferSize*1024;
	on_config(in_cfg, true);
	on_config(out_cfg, false);

	low_water_mark = 0;

	ASSERT(low_water_mark == 0);
	ASSERT(high_water_mark <= buffer_size);
}

#include "cg_fuji_control.hpp"


static std::string
log_str_wrap(const std::string& s)
{
#if 0
	return "std::string(\"LWT \" + std::to_string(schedule_idx) + \"@\" + std::to_string(__LINE__) + \" " + s + "\").c_str()"; 
#else
	return "";
#endif
}


DataGenBufferPosPtr
DataGen::write_buffer_get_pos(const DataGenBufferPtr& buffer, clite::Block* flush_state,
	const clite::ExprPtr& num, const DataGenExprPtr& pred,
	const std::string& dbg_flush)
{
	bool existed = buffer_ensure_exists(buffer.get());
	ASSERT(!existed && "Only one buffer write_pos allowed");

	auto& state = *m_codegen.get_current_state();
	auto& fragment = get_fragment();

	auto write_context = fragment.new_var(unique_id(),
		buffer->parent_class + "::WriteContext",
		clite::Variable::Scope::ThreadWide);

	clite::Builder builder(state);

#if 1
	builder
		<< builder.effect(builder.function("BUFFER_DBG_PRINT",
			builder.reference(buffer->var),
			builder.literal_from_str(log_str_wrap("write buffer begin(" + buffer->c_name + ")"))))
		;
#endif
	builder
		<< builder.effect(builder.function("BUFFER_WRITE_BEGIN", {
			builder.reference(buffer->var), builder.reference(write_context), num
		}))
#if 0		
		<< builder.effect(builder.function("BUFFER_DBG_PRINT", {
			builder.reference(buffer->var), builder.str_literal("write_buffer_get_pos(" + buffer->c_name + ")")
		}))
#endif
		;

	return std::make_shared<DataGenBufferPos>(buffer.get(),
		write_context, flush_state, num, dbg_flush);
}


void
DataGen::write_buffer_commit(const DataGenBufferPosPtr& pos)
{
	auto& state = *m_codegen.get_current_state();

	clite::Builder builder(state);

	auto cond = builder.function("BUFFER_CONTROL_SHALL_FLUSH",
		builder.reference(pos->buffer->var));

	builder
		<< builder.effect(builder.function("BUFFER_WRITE_COMMIT",
			builder.reference(pos->buffer->var), builder.reference(pos->var)))
#if 1

		<< builder.predicated(
			cond,
			builder.effect(builder.function("BUFFER_DBG_PRINT",
				builder.reference(pos->buffer->var),
				builder.literal_from_str(log_str_wrap(": write_buffer_commit(" + pos->buffer->c_name + "): FLUSH " + pos->dbg_flush)))))
#endif
#if 0
		<< builder.effect(builder.function("BUFFER_DBG_PRINT",
			builder.reference(pos->buffer->var), builder.str_literal("write_buffer_commit(" + pos->buffer->c_name + ")")))
#endif
		<< builder.cond_branch(cond, pos->flush_state, clite::BranchLikeliness::Unlikely,
			clite::BranchThreading::NeverYield)
		;
}

bool
DataGen::buffer_ensure_exists(DataGenBuffer* buffer)
{
	const std::string buffer_var(buffer->c_name);

	auto& fragment = get_fragment();

	auto& structs = m_codegen.global_structs;
	if (structs.find(buffer_var) != structs.end())  {
		ASSERT(!buffer->parent_class.empty());
		return true;
	}

	ASSERT(buffer->parent_class.empty());

	buffer->parent_class =
		"SimpleBuf<" + std::to_string(buffer->high_water_mark) + ", " + 
			std::to_string(buffer->low_water_mark) +", " +
			std::to_string(buffer->read_granularity) +", " +
			std::to_string(buffer->write_granularity) +", " +
			std::to_string(buffer->buffer_size) + ">";

	// add current buffer
	auto record = FujiCodegen::Record {FujiCodegen::Record::Pipeline, {},
		buffer->parent_class, buffer->dbg_name};
	structs.insert({buffer_var, std::move(record)});

	buffer->var = fragment.new_outside_var(buffer_var,"S__" + buffer_var);

	return false;
}

bool
DataGen::buffer_ensure_col_exists(const DataGenBufferColPtr& col,
	const std::string& type)
{
	auto& buffer = col->buffer;
	buffer_ensure_exists(buffer);

	auto& items = m_codegen.global_structs[buffer->c_name].items;
	if (items.find(col->var->name) != items.end()) {
		return true;
	}

	if (!type.empty()) {
		items.insert({col->var->name, {"StaticArray<" + type + ", reserved>", "", "", ""}});
	}

	clite::Factory f;
	col->var->default_value = f.literal_from_str(buffer->c_name + "." + col->var->name + ".get()");

	return false;
}


DataGenBufferPosPtr
DataGen::read_buffer_get_pos(const DataGenBufferPtr& buffer, const clite::ExprPtr& num,
	clite::Block* empty_state, const std::string& dbg_refill)
{
	bool existed = buffer_ensure_exists(buffer.get());
	ASSERT(existed);

	auto& state = *m_codegen.get_current_state();
	clite::Builder builder(state);

	auto shall_refill = builder.function("BUFFER_CONTROL_SHALL_REFILL",
		builder.reference(buffer->var));
	builder
#if 0
		<< builder.effect(builder.function("BUFFER_DBG_PRINT",
			builder.reference(buffer->var),
			builder.str_literal("read_buffer_get_pos(" + buffer->c_name + ")")))
#endif
#if 1
		<< builder.predicated(
			shall_refill,
			builder.effect(builder.function("BUFFER_DBG_PRINT",
				builder.reference(buffer->var),
				builder.literal_from_str(log_str_wrap("read_buffer_get_pos(" + buffer->c_name + "): REFILL " + dbg_refill)))))
#endif
		<< builder.cond_branch(
			shall_refill,
			empty_state, clite::BranchLikeliness::Unlikely)
		;

	const std::string id(unique_id());

	auto read_context = get_fragment().new_var(id, buffer->parent_class + "::ReadContext",
		clite::Variable::Scope::ThreadWide);

	builder
		<< builder.effect(builder.function("BUFFER_READ_BEGIN",
			builder.reference(buffer->var), builder.reference(read_context), num));

	return std::make_shared<DataGenBufferPos>(buffer.get(),
		read_context, nullptr, num, "does not exist");
}


void
DataGen::read_buffer_commit(const DataGenBufferPosPtr& pos)
{
	auto& state = *m_codegen.get_current_state();
	clite::Builder builder(state);

	builder
		<< builder.effect(builder.function("BUFFER_READ_COMMIT",
			builder.reference(pos->buffer->var), builder.reference(pos->var)
			))
#if 1
		<< builder.effect(builder.function("BUFFER_DBG_PRINT",
			builder.reference(pos->buffer->var),
			builder.literal_from_str(log_str_wrap("read_buffer_commit(" + pos->buffer->c_name + ")"))))
#endif
		;
}

void
DataGen::on_buffer_non_empty(const DataGenBufferPtr& buffer, clite::Block* state, clite::Block* goto_state,
	const std::string& dbg_name)
{
	const std::string buffer_var(buffer->c_name);

	clite::Builder builder(state);

	auto is_empty = builder.function("BUFFER_IS_EMPTY", builder.reference(buffer->var));

	builder << builder.CLITE_LOG("on_buffer_non_empty " + dbg_name, {});
	if (goto_state) {
		auto cond = builder.function("!", is_empty);
		builder
			<< builder.predicated(cond, builder.CLITE_LOG("GOTO " + dbg_name, {}))
			<< builder.cond_branch(cond, goto_state,
				clite::BranchLikeliness::Likely, clite::BranchThreading::NeverYield)
			;
	} else {
		builder
			<< builder.effect(builder.function("ASSERT", is_empty))
			<< builder.log_debug("final flush", {})
			;
	}
}

void
DataGen::assert_buffer_empty(const DataGenBufferPtr& buffer, clite::Block* state)
{
	return on_buffer_non_empty(buffer, state, nullptr, "assert_empty");
}

DataGen::PrefetchType
DataGen::can_prefetch(const ExprPtr& e)
{
	if (e->is_aggr() || str_in_strings(e->fun, {
		"bucket_lookup", "bucket_next" , "gather", "check"})) {

		if (!str_in_strings(e->fun, {"aggr_gcount", "aggr_gsum"})) {
			return DataGen::PrefetchType::PrefetchBefore;
		}
	}

	return DataGen::PrefetchType::None;
}

void
DataGen::prefetch(const ExprPtr& expr, int temporality)
{
	ASSERT(false && "todo");
}


ExprPtr
DataGen::get_bucket_index(const ExprPtr& e)
{
	if (str_in_strings(e->fun, {"gather", "scatter", "check", "bucket_next"})) {
		return e->args[1];
	}

	if (e->is_aggr()) return e->args[1];

	ASSERT(false);
	return nullptr;
}

std::string
DataGen::get_flavor_name() const
{
	ASSERT(false);
	return "???";
}

clite::ExprPtr
DataGen::is_predicate_non_zero(const DataGenExprPtr& pred)
{
	clite::Factory f;
	return f.function("!!", f.reference(pred->var));
}

clite::Fragment&
DataGen::get_fragment()
{
	return m_codegen.m_flow_gen->fragment;		
}

void
DataGen::buffer_overwrite_mask(const DataGenExprPtr& c, const DataGenBufferPosPtr& mask)
{
}


clite::VarPtr
DataGen::new_global_aggregate(const std::string& tpe,
	const std::string& tbl, const std::string& col, const std::string& comb_type)
{
	clite::Factory factory;
	auto id = unique_id();
	auto acc = get_fragment().new_var(id, tpe,
		clite::Variable::Scope::ThreadWide, false, "0");
	
	if (!comb_type.empty()) {
		ASSERT(m_codegen.global_agg_bucket);
		auto fetched = access_column(tbl, kColPrefix + col,
			m_codegen.global_agg_bucket);

		auto hash = access_column(tbl, "hash",
			m_codegen.global_agg_bucket);

		m_codegen.at_fin.push_back(factory.assign(hash,
			factory.literal_from_str(std::to_string(kGlobalAggrHashVal))));

		m_codegen.at_fin.push_back(factory.effect(
			factory.function("SCALAR_AGGREGATE",
				factory.literal_from_str(comb_type), fetched, factory.reference(acc)
			)
		));			
		m_codegen.at_fin.push_back(factory.assign(acc, factory.literal_from_str("0")));
	}

	return acc;
}

DataGenBufferCol::DataGenBufferCol(DataGenBuffer* buffer, const clite::VarPtr& var,
	const std::string& scal_type, const bool predicate)
 : var(var), buffer(buffer), predicate(predicate), scal_type(scal_type) {
}
