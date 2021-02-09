#ifndef H_CG_FUJI
#define H_CG_FUJI

#include <string>
#include <vector>
#include <memory>
#include <stack>
#include "clite.hpp"
#include "cg_fuji_data.hpp"
#include "codegen.hpp"
#include <unordered_map>

struct FlowGenerator;
struct Pipeline;
struct Lolepop;
struct Statement;
struct Expression;
struct State;
struct BlendContext;

struct FujiCodegen : Codegen {
	void gen_pipeline(Pipeline& p, size_t number) override;
	void gen_lolepop(Lolepop& l, Pipeline& p) override;

	// expression cache
	std::unordered_map<ExprPtr, DataGenExprPtr> exprs;

private:
	// variable resolution, Blend requires us to maintain a stack
	std::unordered_map<std::string, std::stack<DataGenExprPtr>> m_vars;

	DataGenExprPtr get_current_var(const std::string& s);

	void new_variable(const std::string& s, const DataGenExprPtr& e);


	// operator inputs
	std::vector<std::stack<DataGenExprPtr>> m_op_tuple;

	DataGenExprPtr get_lolepop_arg(const size_t idx);
	size_t get_lolepop_num_args() const;

public:

	std::unordered_map<ExprPtr, BlendContext*> expr_blends;

	BlendContext* m_current_blend = nullptr;

	BlendContext* create_blend_context(BlendConfigPtr config, BlendContext* prev) {
		return create_blend_context(config.get(), prev);
	}

	BlendContext* create_blend_context(const BlendConfig* const config, BlendContext* prev);


	// modify expression cache

	void put(const ExprPtr& e, const DataGenExprPtr& expr);
	DataGenExprPtr get_ptr(const ExprPtr& e);

	FujiCodegen(QueryConfig& config);
	~FujiCodegen();

	clite::Block* get_current_state() const;
private:
	void gen(StmtPtr& e);

	DataGenExprPtr gen(ExprPtr& e, bool pred);
	DataGenExprPtr _gen(ExprPtr& e, bool pred);
	auto gen_expr(ExprPtr& e) { return gen(e, false); }
	auto gen_pred(ExprPtr& e) { return gen(e, true); }

	void gen_blend(BlendStmt& blend_op, const BlendConfig* new_blend_config);

	std::stack<DataGenExprPtr> m_lolepred;

public:
	std::unique_ptr<FlowGenerator> m_flow_gen;

private:
	struct OpCtx {		
		Pipeline& pipeline;
		Lolepop& lolepop;

		OpCtx(Pipeline& p, Lolepop& l);
 	};

 	// current operator, allocated on stack
	OpCtx* m_current_op = nullptr;
	size_t m_pipeline_id = 0;

	size_t loleid_counter = 0;
	size_t lolepop_count = 0;
	std::string get_lolepop_id(Lolepop& l, const std::string& n);
	std::string get_lolepop_id(const std::string& n);

	const BlendSpacePoint::Pipeline* m_pipeline_space_point;
	size_t m_blends_done = 0; //!< Per-pipeline counter of blend space points consumed

public:
	bool concurrent_fsms_disabled() const;

	void register_reset_var(const clite::VarPtr& p, const clite::ExprPtr& val) {
		clite::Factory factory;
		m_reset_vars.push_back({ p, factory.assign(p, val) });
	}

	void register_reset_var_vec(const clite::VarPtr& p) {
		clite::Factory factory;
		m_reset_vars.push_back({ p,
			factory.effect(factory.function("IFujiVector::RESET",
				factory.reference(p))) });
	}

private:
	struct BufferInfo {
		DataGenBufferPtr buffer;
		clite::Block* flush_state;

		BlendConfig* blend_config;
	};
	std::vector<BufferInfo> m_buffers;

	clite::Block* m_inflow_back_edge;
	size_t m_inflow_back_edge_generated;

	clite::Block* m_flush_back_edge = nullptr;
	DataGenBufferPtr m_flush_buffer = nullptr;

	struct LoopBackedge {
		clite::Block* first_buffer_write_followup;
		clite::Block* last_buffer_flush;
		DataGenBufferPtr last_buffer;
		DataGen* data_gen;

		clite::Block* inflow_back_edge;

		LoopBackedge() {
			first_buffer_write_followup = nullptr;
			last_buffer_flush = nullptr;
			last_buffer = nullptr;
			data_gen = nullptr;
			inflow_back_edge = nullptr;
		}
	};
	std::stack<LoopBackedge> m_loop_backedges;


	// reigsrered with register_reset_var()
	struct ResetVar {
		clite::VarPtr var;
		clite::StmtPtr stmt;
	};
	std::vector<ResetVar> m_reset_vars;

	// other

	void gen_flush_backedge(const DataGenBufferPtr& buffer, DataGen& gen,
		clite::Block* followup, clite::Block* flush_state, clite::Block* inflow_back_edge,
		const std::string& dbg_name);
	void gen_flush_backedge(const LoopBackedge& lbe);

	std::vector<BlendContext*> m_blend_context;
public:
	struct Record {
		struct Item {
			std::string type;
			std::string init = "";
			std::string deinit = "";
			std::string post_type = "";
			bool meta = false;
			bool constant_expr = false;
		};

		enum Scope {
			Pipeline
		};

		Scope scope;

		std::unordered_map<std::string, Item> items;

		std::string parent_class;
		std::string dbg_name;
	};

	std::unordered_map<std::string, Record> global_structs;

	void gen_global_structs(std::ostringstream& out, std::ostringstream& reg, Record::Scope scope);

	clite::ExprPtr global_agg_bucket;
	clite::StmtList at_fin;
	std::string at_begin;

#ifdef TRACE
	std::vector<std::string> key_checks;
#endif
};


#endif
