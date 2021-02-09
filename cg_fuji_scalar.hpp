#ifndef H_CG_FUJI_SCALAR
#define H_CG_FUJI_SCALAR

#include "cg_fuji_data.hpp"
#include "voila.hpp"
#include <sstream>

struct FujiCodegen;

struct ScalarDataGen : DataGen {
	struct ScalarExpr : DataGenExpr {
		ScalarExpr(DataGen& gen, const clite::VarPtr& v, bool predicate) 
		 : DataGenExpr(gen, v, predicate) {}
	};

	ScalarDataGen(FujiCodegen& cg) : DataGen(cg, "scalar") {
		m_unroll_factor = 1;
	}

	ScalarExpr* get(const ExprPtr& e);

	DataGenExprPtr new_simple_expr(const DataGen::SimpleExprArgs& args) override;

	DataGenExprPtr new_non_selective_predicate() override;

	DataGenExprPtr clone_expr(const DataGenExprPtr& a) override;

	void copy_expr(DataGenExprPtr& _res, const DataGenExprPtr& _a) override;

	void gen_output(StmtPtr& e) override;
	DataGenExprPtr gen_emit(StmtPtr& e, Lolepop* op) override;
	void gen_extra(clite::StmtList& stmts, ExprPtr& e) override;

	void gen_expr(ExprPtr& e) override;
	void gen_pred(ExprPtr& e) override;
	
	DataGenBufferPosPtr write_buffer_get_pos(const DataGenBufferPtr& buffer, clite::Block* flush_state,
		const clite::ExprPtr& num, const DataGenExprPtr& pred, const std::string& dbg_flush) override;
	DataGenBufferColPtr write_buffer_write_col(const DataGenBufferPosPtr& pos,
		const DataGenExprPtr& expr, const std::string& dbg_name) override;

	DataGenBufferPosPtr read_buffer_get_pos(const DataGenBufferPtr& buffer,
		const clite::ExprPtr& num, clite::Block* empty,
		const std::string& dbg_refill) override;
	DataGenExprPtr read_buffer_read_col(const DataGenBufferPosPtr& pos,
		const DataGenBufferColPtr& buffer, const std::string& dbg_name) override;

	void prefetch(const ExprPtr& e, int temporality) override;

	std::string get_flavor_name() const override;
private:
	void gen(ExprPtr& e, bool pred);


	clite::VarPtr expr2get0(ExprPtr& arg);

	clite::VarPtr get_bucket_index_var(const ExprPtr& e) {
		auto r = get_bucket_index(e);
		return expr2get0(r);
	}

	clite::ExprPtr get_bucket_index_expr(const ExprPtr& e) {
		clite::Factory f;
		return f.reference(get_bucket_index_var(e));
	}
};



#endif