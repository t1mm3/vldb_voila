#include "pass.hpp"
#include "voila.hpp"
#include "utils.hpp"
#include "runtime.hpp"

#include <sstream>

Lolepop*
IPass::move_to_next_op(Pipeline& p)
{
	auto r = get_next_op(p);
	if (!r) {
		return nullptr;
	}

	if (push_driven) {
		lolepop_idx++;
	} else {
		lolepop_idx--;
	}
	return p.lolepops[lolepop_idx].get();
}

Lolepop*
IPass::get_next_op(Pipeline& p)
{
	if (push_driven) {
		if (lolepop_idx+1 >= p.lolepops.size()) {
			return nullptr;
		}

		return p.lolepops[lolepop_idx+1].get();
	} else {
		if (lolepop_idx == 0) {
			return nullptr;
		}
		return p.lolepops[lolepop_idx-1].get();
	}
}

Lolepop*
IPass::get_prev_op(Pipeline& p)
{
	if (push_driven) {
		if (lolepop_idx > 0) {
			return p.lolepops[lolepop_idx-1].get();
		}
	} else {
		if (lolepop_idx+1 < p.lolepops.size()) {
			return p.lolepops[lolepop_idx+1].get();
		}
	}
	return nullptr;
}

std::string
IPass::var_name(const LolepopCtx& ctx, const std::string& id)
{
	std::ostringstream r;
	r << ctx.l.name << "_" << id;
	return r.str();
}


#include <atomic>
static std::atomic<uint64_t> id_counter;

std::string
IPass::unique_id(const std::string& prefix, const std::string& postfix)
{
	std::ostringstream r;
	r << prefix << id_counter++ << postfix;
	return r.str();
}




void
Pass::recurse_pipeline(Pipeline& p, RecurseFlags flags)
{
	(void)flags;

	auto& ls = p.lolepops;
	size_t num = p.lolepops.size();

	if (flat) {
		for (auto& l : p.lolepops) {
			ASSERT(l);
			on_lolepop(p, *l);
		}
		return;
	}

	lolepop_idx = 0;

	ASSERT(num > 0);
	if (push_driven) {
		ASSERT(ls[0]);
		on_lolepop(p, *ls[0]);
	} else {
		ASSERT(ls[num-1]);
		on_lolepop(p, *ls[num-1]);
	}
}

void
Pass::recurse_lolepop(Pipeline& p, Lolepop& l, RecurseFlags flags)
{
	(void)flags;

	for (auto& s : l.statements) {
		LolepopCtx ctx { p, l};

		ASSERT(s);
		on_statement(ctx, s);
	}
}

void
Pass::recurse_statement(LolepopCtx& ctx, StmtPtr& stmt, bool pre, RecurseFlags flags)
{
	auto s = *stmt;
	Lolepop* next;

	if ((flags & kRecurse_Pred) && pre && s.pred) {
		on_expression(ctx, s.pred);
	} 

	switch (s.type) {
	case Statement::Type::Loop:
	case Statement::Type::Wrap:
	case Statement::Type::BlendStmt: 
		if (pre) {
			if (s.expr) {
				on_expression(ctx, s.expr);
			}
		} else {
			for (auto& stmt : s.statements) {
				on_statement(ctx, stmt);
			}
		}
		break;
	case Statement::Type::Assignment:
	case Statement::Type::EffectExpr:
		if (pre) {
			on_expression(ctx, s.expr);
		}
		break;

	case Statement::Type::Emit:
		if (pre) {
			on_expression(ctx, s.expr);
		} else {
			if (!flat) {
				next = move_to_next_op(ctx.p);
				if (next) {
					on_lolepop(ctx.p, *next);
				}
			}
		}
		break;

	case Statement::Type::MetaStmt:
		if (!pre) {
			for (auto& stmt : s.statements) {
				on_statement(ctx, stmt);
			}
		}
		break;
	case Statement::Type::Done:
		break;

	default:
		ASSERT(false);
		break;
	}
}

void
Pass::recurse_expression(LolepopCtx& ctx, ExprPtr& e, RecurseFlags flags)
{
	if ((flags & kRecurse_Pred) && e->pred) {
		on_expression(ctx, e->pred);
	}

	if (flags & kRecurse_ExprArgs) {
		for (auto& arg : e->args) {
			ASSERT(arg);
			on_expression(ctx, arg);
		}
	}
}

void
Pass::recurse_program(Program& p, RecurseFlags flags) {
	(void)flags;

	for (auto& d : p.data_structures) {
		on_data_structure(d);
	}

	for (auto& pl : p.pipelines) {
		on_pipeline(pl);
	}

}

void
Pass::operator()(Program& p)
{
	on_program(p);
	on_done();
}

void PassPipeline::operator()(Program& p)
{
	for (auto& pass : passes) {
		ASSERT(pass);
		(*pass)(p);
	}
}




void
RefCountingPass::add_expr(ExprPtr& e, NodePtr source)
{
	expr_nodes[e].push_back(source);
}

void
RefCountingPass::add_var(const std::string& s, NodePtr source)
{
	var_nodes[s].push_back(source);
}

void
RefCountingPass::on_pipeline(Pipeline& p) {
	lole_args.clear();
	recurse_pipeline(p);
}

void
RefCountingPass::on_statement(IPass::LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);

	auto expr = [&] (auto& arg) {
		add_expr(arg, std::static_pointer_cast<Node>(s));
	};

	if (s->expr) {
		expr(s->expr);
	}

	if (s->pred) {
		expr(s->pred);
	}

	switch (s->type) {
	case Statement::Type::Assignment:
		add_var(s->var, std::static_pointer_cast<Node>(s));
		break;

	case Statement::Type::Emit:
		ASSERT(s->expr->type == Expression::Type::Function && !s->expr->fun.compare("tappend"));
		lole_args = s->expr->args;
		break;
	default:
		break;
	}
}

void
RefCountingPass::on_expression(IPass::LolepopCtx& ctx, ExprPtr& s)
{
	recurse_expression(ctx, s);

	auto expr = [&] (ExprPtr& arg) {
		add_expr(arg, s);
	};

	if (s->pred) {
		expr(s->pred);
	}

	for (auto& arg : s->args) {
		expr(arg);	
	}

	switch (s->type) {
	case Expression::Type::Reference:
		add_var(s->fun, s);
		break;

	case Expression::Type::Function:
		if (!s->fun.compare("tget")) {
			auto idx = TupleGet::get_idx(*s);

			if (s->args[0]->type == Expression::Type::LoleArg) {
				expr(lole_args[idx]);
			} else {
				ASSERT(false && "todo");
			}
		}
		break;

	default:
		break;
	}
}

size_t
RefCountingPass::get_count(const ExprPtr& e) const
{
	auto it = expr_nodes.find(e);
	ASSERT(it != expr_nodes.end());
	return it->second.size();
}

size_t
RefCountingPass::get_count(const std::string& e) const
{
	auto it = var_nodes.find(e);
	ASSERT(it != var_nodes.end());
	return it->second.size();
}