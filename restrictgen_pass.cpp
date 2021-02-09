#include "restrictgen_pass.hpp"
#include "runtime.hpp"

void
RestrictGenPass::on_expression(LolepopCtx& ctx, ExprPtr& expr)
{
	recurse_expression(ctx, expr);
	auto& e = *expr;
	const auto& n = e.fun;

	auto range = [&] (size_t begin, size_t end) {
		const auto n = e.args.size();
		ASSERT(begin < n);
		ASSERT(end <= n);
		e.props.gen_recurse.resize(n);
		for (size_t i=0; i<n; i++) {
			e.props.gen_recurse[i] = true;
		}
		for (size_t i=begin; i<end; i++) {
			e.props.gen_recurse[i] = false;
		}
	};

	auto all_args = [&] () { range(0, e.args.size()); };

	auto spec = [&] (auto i) { range(i, i+1); };

	if (!n.compare("write_pos")) {
		spec(0);
		return;
	}
	// relation
	if (e.is_get_morsel()) {
		spec(0);
		return;
	}

#if 0
	if (e.is_get_pos() && e.fun.compare("scan_pos")) {
		spec(0);
		return;
	}
#endif

	if (!n.compare("tget")) {
		spec(1);
		return;
	}

	if (!n.compare("scan") || e.is_table_op()) {
		spec(0);
		return;
	}
}
