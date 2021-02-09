#ifndef H_CG_FUJI_VECTOR
#define H_CG_FUJI_VECTOR

#include "cg_fuji_data.hpp"
#include "voila.hpp"
#include <sstream>
#include <stack>

struct FujiCodegen;

struct VectorDataGen : DataGen {
	struct VectorExpr : DataGenExpr {
		VectorExpr(DataGen& gen, const clite::VarPtr& v,
				bool predicate, const std::string& source,
				bool scalar, bool is_num, const std::string& scal_type) 
		 : DataGenExpr(gen, v, predicate), source(source),
		 scalar(scalar), is_num(is_num), scal_type(scal_type) {}

		DataGenExprPtr num;

		const std::string source;

		VectorExpr* clone_origin = nullptr;

		const bool scalar;
		const bool is_num;
		const std::string scal_type;


		virtual clite::ExprPtr get_len() override {
			clite::Factory f;
			if (scalar) {
				return f.reference(var);
			}

			if (num) {
				return f.reference(num->var);
			}

			auto& gen = (VectorDataGen&)data_gen;

			return f.reference(gen.const_vector_size());
		}
	};

	VectorDataGen(FujiCodegen& cg, size_t vsize)
	 : DataGen(cg, "vector(" + std::to_string(vsize) + ")") {
		m_unroll_factor = vsize;
	}

	VectorExpr* get(const ExprPtr& e);

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
		const clite::ExprPtr& num, clite::Block* empty, const std::string& dbg_refill) override;
	DataGenExprPtr read_buffer_read_col(const DataGenBufferPosPtr& pos,
		const DataGenBufferColPtr& buffer, const std::string& dbg_name) override;

	void prefetch(const ExprPtr& e, int temporality) override;

	std::string get_flavor_name() const override;
	clite::ExprPtr is_predicate_non_zero(const DataGenExprPtr& _pred) override;

	// Input columns and lole-pred are special, we track them to avoid emiting "&" in front of expressions
	std::unordered_map<ExprPtr, clite::VarPtr> special_col;
private:
	void gen(ExprPtr& e, bool pred);


	clite::VarPtr expr2get0(const ExprPtr& arg);

	clite::VarPtr get_bucket_index_var(const ExprPtr& e) {
		auto r = get_bucket_index(e);
		return expr2get0(r);
	}

	clite::ExprPtr get_bucket_index_expr(const ExprPtr& e) {
		clite::Factory f;
		return f.reference(get_bucket_index_var(e));
	}


	clite::VarPtr const_vector_size();
	clite::VarPtr var_const_vector_size;
};



#endif