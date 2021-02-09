#ifndef H_CG_FUJI_AVX512
#define H_CG_FUJI_AVX512

#include "cg_fuji_data.hpp"
#include "voila.hpp"

struct Avx512DataGen : DataGen {
	struct SimdExpr : DataGenExpr {
		DataGenExprPtr mask;
		std::string scalar_type;

		SimdExpr(DataGen& dgen, const clite::VarPtr& var, bool predicate)
		 : DataGenExpr(dgen, var, predicate) {}

		virtual clite::ExprPtr get_len() override {
			clite::Factory f;
			return f.function("popcount32", f.reference(var));
		}
	};
	struct Mask : SimdExpr {
		Mask(DataGen& dgen, const clite::VarPtr& var);
	};


	Avx512DataGen(FujiCodegen& cg) : DataGen(cg, "avx512") {
		m_unroll_factor = 8;
	}

	DataGenExprPtr new_simple_expr(const SimpleExprArgs& args) override;

	DataGenExprPtr new_non_selective_predicate() override;

	DataGenExprPtr clone_expr(const DataGenExprPtr& a) override;
	void copy_expr(DataGenExprPtr& _res, const DataGenExprPtr& _a) override;

	SimdExpr* get(const ExprPtr& e);

	static clite::ExprPtr read_arg(const std::string& type,
		const std::string& base_type, const clite::VarPtr& var,
		int idx);
	clite::ExprPtr read_arg(ExprPtr& e, int idx);

	static clite::StmtPtr write_arg(const std::string& type,
		const std::string& base_type, const clite::VarPtr& var,
		int idx, const clite::ExprPtr& value);
	clite::StmtPtr write_arg(ExprPtr& e, int idx, const clite::ExprPtr& value);	

	void gen_expr(ExprPtr& e) override;
	void gen_pred(ExprPtr& e) override;
	void gen_output(StmtPtr& e) override;
	DataGenExprPtr gen_emit(StmtPtr& e, Lolepop* op) override;
	void gen_extra(clite::StmtList& stmts, ExprPtr& e) override;

	DataGenBufferPosPtr write_buffer_get_pos(const DataGenBufferPtr& buffer, clite::Block* flush_state,
		const clite::ExprPtr& num, const DataGenExprPtr& pred, const std::string& dbg_flush) override;
	DataGenBufferPosPtr read_buffer_get_pos(const DataGenBufferPtr& buffer, const clite::ExprPtr& num,
		clite::Block* empty, const std::string& dbg_refill) override;
	DataGenBufferColPtr write_buffer_write_col(const DataGenBufferPosPtr& pos,
		const DataGenExprPtr& expr, const std::string& dbg_name) override;

	DataGenExprPtr read_buffer_read_col(const DataGenBufferPosPtr& pos,
		const DataGenBufferColPtr& buffer, const std::string& dbg_name) override;


	DataGen::PrefetchType can_prefetch(const ExprPtr& expr) override;
	void prefetch(const ExprPtr& e, int temporality) override;
	std::string get_flavor_name() const override;

	void buffer_overwrite_mask(const DataGenExprPtr& c, const DataGenBufferPosPtr& mask) override;
private:
	void gen(ExprPtr& e, bool pred);

	std::string get_simd_type(const std::string& res0_type) const;

	static clite::VarPtr direct_get_predicate(const clite::VarPtr& pred, int idx) {
		return pred;
	}
	clite::VarPtr direct_get_predicate(const ExprPtr& pred, int idx);
	clite::VarPtr direct_get_predicate(const SimdExpr* pred, int idx);

	clite::VarPtr wrap_get_predicate(const ExprPtr& e, int idx) {
		return direct_get_predicate(e->pred, idx);
	}

	clite::VarPtr wrap_get_predicate(const clite::VarPtr& e, int idx) {
		return direct_get_predicate(e, idx);
	}

	clite::StmtList direct_wrap_predicated(const clite::VarPtr& pred, const clite::StmtList& sts, int idx);
	clite::StmtList direct_wrap_predicated(const SimdExpr* pred, const clite::StmtList& sts, int idx);
	clite::StmtList direct_wrap_predicated(const ExprPtr& pred, const clite::StmtList& sts, int idx);

	clite::StmtList wrap_predicated(const ExprPtr& e,
		const clite::StmtList& sts, int idx)
	{
		return direct_wrap_predicated(e->pred, sts, idx);
	}

	clite::StmtList wrap_predicated(const clite::VarPtr& var,
		const clite::StmtList& sts, int idx)
	{
		return direct_wrap_predicated(var, sts, idx);
	}

	template<typename T>
	clite::StmtList unroll(const T& f) {
		clite::Factory factory;
		clite::StmtList r;
		for (size_t i=0; i<get_unroll_factor(); i++) {
			r.emplace_back(factory.scope(f(i)));
		}
		return r;
	}


	typedef int UnrollFlags;
	static const UnrollFlags kUnrollOptimistic = 1 << 2;
	static const UnrollFlags kUnrollNoAdaptive = 1 << 3;
	static const UnrollFlags kUnrollNoNop = 1 << 4;

	static const UnrollFlags kUnrollNop = 1 << 6;
	static const UnrollFlags kUnrollAdaptive = 1 << 7;

	template<typename S, typename T>
	clite::StmtList unroll(const S& e, const T& f, UnrollFlags flags = 0) {
		clite::StmtList result;

		// flags |= kUnrollNop;
		flags |= kUnrollAdaptive;
		
		auto pred = wrap_get_predicate(e, 0);
		if ((!(flags & kUnrollNoAdaptive)) && pred) { // (flags & kUnrollAdaptive) && 
			clite::Factory factory;

			clite::ExprPtr opti_cond;
			clite::ExprPtr pessi_cond;
			clite::ExprPtr nop_cond;

			if (flags & kUnrollOptimistic) {
				auto popcnt = factory.function("popcount32", factory.reference(pred));
				opti_cond = factory.function(">=",
					popcnt,
					factory.literal_from_int(7));
			} else {
				opti_cond = factory.function("==",
					factory.reference(pred), factory.literal_from_str("0xFF"));
			}
			pessi_cond = factory.function("!", opti_cond);

			if (!(flags & kUnrollNoNop)) {
				nop_cond = factory.function("==", factory.literal_from_str("0"), factory.reference(pred));
			}

			clite::StmtList path_opti = unroll([&] (int i) {
				return f(i);
			});

			clite::StmtList path_pessi = unroll([&] (int i) {
				return wrap_predicated(e, f(i), i);
			});

			if (nop_cond) {
				pessi_cond = factory.function("&&", pessi_cond, factory.function("!", nop_cond));
			}

			result.emplace_back(factory.predicated(opti_cond,
				std::move(path_opti)));
			result.emplace_back(factory.predicated(pessi_cond,
				std::move(path_pessi)));
		} else {
			result = unroll([&] (int i) {
				if (flags & kUnrollOptimistic) {
					return f(i);
				}
				return wrap_predicated(e, f(i), i);
			});
		}

		return result;
	}

	template<typename S, typename T>
	void unrolled(clite::StmtList& list, const S& e, const T& f, UnrollFlags flags = 0) {
		clite::Factory factory;
		list.emplace_back(factory.scope(unroll<S, T>(e, f, flags)));
	}

	static clite::ExprPtr access_table(const std::string& tbl) {
		clite::Factory factory;
		return factory.literal_from_str("thread." + tbl);
	}

	static clite::ExprPtr get_pred(const clite::ExprPtr& mask, int idx);
	static clite::ExprPtr get_pred(const clite::VarPtr& mask, int idx);
};



#endif