#ifndef H_RESTRICTGEN_PASS
#define H_RESTRICTGEN_PASS

#include "pass.hpp"

struct RestrictGenPass : Pass {
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;	
};

#endif