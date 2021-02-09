#include "propagate_predicates_pass.hpp"
#include "printing_pass.hpp"
#include "runtime.hpp"
#include "voila.hpp"
#include "utils.hpp"

void
LoopReferences::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	const bool loop = s->type == Statement::Type::Loop;

	if (loop) {
		loops.push_back(s);
	}

	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);

	if (s->type == Statement::Type::Assignment) {
		for (auto& loop : loops) {
			ASSERT(loop);
			loop->props.loop_refs.insert(s->var);
		}
	}

	if (loop) {
		ASSERT(loops.back() == s);
		loops.pop_back();
	}
}

void
LoopReferences::on_expression(LolepopCtx& ctx, ExprPtr& s)
{
	recurse_expression(ctx, s);

	if (s->type == Expression::Type::Reference) {
		for (auto& loop : loops) {
			ASSERT(loop);
			loop->props.loop_refs.insert(s->fun);
		}
	}
}

void
DisambiguatePass::on_lolepop(Pipeline& p, Lolepop& l)
{
	vars.clear();
	recurse_lolepop(p, l);
}

void
DisambiguatePass::on_statement(IPass::LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);

	switch (s->type) {
	case Statement::Type::Assignment:
		vars.insert(s->var);
		s->var = ctx.l.name + "__" + s->var;
		break;

	case Statement::Type::MetaStmt:
		{
			auto meta_node = (MetaStmt*)s.get();
			if (meta_node->meta_type == MetaStmt::MetaType::VarDead) {
				auto var_dead = (MetaVarDead*)meta_node;
				var_dead->variable_name = ctx.l.name + "__" + var_dead->variable_name;
			}
			break;
		}

	default:
		break;
	}
}

void
DisambiguatePass::on_expression(IPass::LolepopCtx& ctx, ExprPtr& s)
{
	recurse_expression(ctx, s);

	if (s->type != Expression::Type::Reference) {
		return;
	}

	const std::string new_name(ctx.l.name + "__" + s->fun);

	auto it = vars.find(s->fun);
	if (it == vars.end()) {
		// printf("DisambiguatePass didn't find '%s' .. ignoring\n.", s->fun.c_str());
		// ignore unknown variables
		return;
	}
	s->fun = new_name;
}

void
DisambiguatePass::on_program(Program& p)
{
	flat = true;
	recurse_program(p);

	LOG_DEBUG("================ DisambiguatePass ==================\n");
	PrintingPass printer;
	printer(p);
	LOG_DEBUG("%s\n", printer.str().c_str());
	LOG_DEBUG("================ /DisambiguatePass ==================\n");
}


void
AnnotateVectorCardinalityPass::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);

	switch (s->type) {
	case Statement::Loop:
	case Statement::EffectExpr:
	case Statement::Emit:
	case Statement::Done:
	case Statement::Wrap:
	case Statement::BlendStmt:
	case Statement::MetaStmt:
		break;

	case Statement::Assignment:
		vars.insert({s->var, s->expr});
		break;
	default:
		ASSERT(false);
		break;
	};

	if (s->expr) {
		s->props.vec_cardinality = s->expr->props.vec_cardinality;
	}
	if (s->pred) {
		s->props.vec_cardinality = s->pred->props.vec_cardinality;	
	}

	recurse_statement(ctx, s, false);
}

void
AnnotateVectorCardinalityPass::on_expression(LolepopCtx& ctx, ExprPtr& s)
{
	// already done?
	if (traversed.find(s) != traversed.end()) {
		return;
	}

	recurse_expression(ctx, s);

	auto set = [&] (ExprPtr& card, const char* txt) {
		if (trace && txt) {
			printf("AnnotateVectorCardinalityPass::on_expression: set card %s for %p '%s' to %p '%s'\n",
				txt, s.get(), s->fun.c_str(), card.get(), card ? card->fun.c_str() : "");
		}
		s->props.vec_cardinality = card;
	};


	if (s->pred) {
		set(s->pred, nullptr);
		return;
	}

	switch (s->type) {
	case Expression::Type::Reference:
		{
			if (trace) {
				printf("AnnotateVectorCardinalityPass: Reference '%s'\n", s->fun.c_str());
			}
			const auto it = vars.find(s->fun);

			if (it != vars.end()) {
				set(it->second, nullptr);
			}
		}
		break;
	case Expression::Type::Function:
		{
			auto is_special = [] (auto s) {
				auto& ar = s->props.type.arity;

				if (ar.size() != 1) {
					return false;
				}
				return str_in_strings(ar[0].type, { "Position", "Morsel" } );
			};
			// printf("AnnotateVectorCardinalityPass: Function '%s'\n", s->fun.c_str());

			if (is_special(s)) {
				if (trace) {
					printf("AnnotateVectorCardinalityPass: special %s\n", s->fun.c_str());
				}
				break;
			}

			if (trace) {
				printf("AnnotateVectorCardinalityPass: regular %s\n", s->fun.c_str());
			}

			// assign from input variables
			for (auto& arg : s->args) {
				if (s->props.vec_cardinality) {
					continue;
				}

				if (!arg->props.vec_cardinality) {
					continue;
				}
				set(arg->props.vec_cardinality, "normal arg");
			}

			// overwrite for special (*_pos, *_morsel)
			size_t n = 0;
			for (auto& arg : s->args) {
				if (!is_special(arg)) {
					continue;
				}

				if (trace) {
					printf("AnnotateVectorCardinalityPass: set special card for '%s'\n", s->fun.c_str());
				}
				set(arg, "special arg");
				n++;
			}
			ASSERT(n < 2 && "Only one or zero *_pos or *_morsel allowed");
		}

		break;
	case Expression::Type::LoleArg:
	case Expression::Type::LolePred:
	case Expression::Type::Constant:
		break;
	default:
		ASSERT(false);
		break;	
	}

	traversed.insert(s);
}


#if 0
void
PropagatePredicates::on_lolepop(Pipeline& p, Lolepop& l)
{
	vars.clear();
	recurse_lolepop(p, l);
}

void
PropagatePredicates::on_statement(IPass::LolepopCtx& ctx, Statement& s)
{
	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);

	if (s.type != Statement::Type::Assignment) {
		return;
	}

	ASSERT(s.expr);

	printf("PropagatePredicates: Assignment '%s'\n", s.var.c_str());
	if (s.pred) {
		add_preds(s, s.pred->props.predicate_lineage, "Stmt Pred Lin");
	}
	if (s.expr && s.expr->pred) {
		add_preds(s, s.expr->pred->props.predicate_lineage, "Stmt Expr Pred Lin");
	}
	if (s.expr && !s.pred) {
		s.pred = s.expr->pred;
	}

	vars[s.var] = Ref {&s};
}

void
PropagatePredicates::on_expression(IPass::LolepopCtx& ctx, Expression& s)
{
	recurse_expression(ctx, s);

	switch (s.type) {
	case Expression::Type::Reference:
		{
			printf("PropagatePredicates: Reference '%s'\n", s.fun.c_str());
			const auto it = vars.find(s.fun);

			if (it == vars.end()) {
				break;
			}
			add_preds(s, it->second.stmt->props.predicate_lineage, "Ref Lin");
		}
		break;
	case Expression::Type::Function:
		printf("PropagatePredicates: Function '%s'\n", s.fun.c_str());
		for (auto& arg : s.args) {
			if (!arg) {
				continue;
			}
			add_preds(s, {arg->pred}, "Fun Pred");
			add_preds(s, arg->props.predicate_lineage, "Fun Lin");
		}
		break;
	case Expression::Type::LoleArg:
	case Expression::Type::LolePred:
		printf("PropagatePredicates: Other '%s'\n", s.fun.c_str());
		break;
	case Expression::Type::Constant:
		// do not matter
		break;
	default:
		ASSERT(false);
		break;	
	}
}

void
PropagatePredicates::add_preds(Node& n, const std::vector<Expression*>& ps,
	const char* dbg)
{
	for (auto& p : ps) {
		if (p) {
			printf("PropagatePredicates::add_preds(%p, %s): %p\n", &n, dbg, p);
			n.props.predicate_lineage.push_back(p);
		}
	}
}

void
PropagatePredicates::on_done()
{
	ASSERT(false);
}
#endif

void
UsedVariables::on_program(Program& p)
{
	flat = true;
	recurse_program(p);
}

void
UsedVariables::on_lolepop(Pipeline& p, Lolepop& l)
{
	flat = true;
	inflight.clear();

	recurse_lolepop(p, l);
}

void
UsedVariables::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	flat = true;
	recurse_statement(ctx, s, true);

	switch (s->type) {
	case Statement::Type::Assignment:
		inflight[s->var].writes.insert(s);
		break;

	case Statement::Type::MetaStmt:
		{
			auto meta_node = (MetaStmt*)s.get();
			if (meta_node->meta_type == MetaStmt::MetaType::VarDead) {
				auto var_dead = (MetaVarDead*)meta_node;
				inflight.erase(var_dead->variable_name);
			}
			break;
		}
	default:
		break;
	} 

	recurse_statement(ctx, s, false);
}

void
UsedVariables::on_expression(LolepopCtx& ctx, ExprPtr& s)
{
	flat = true;
	if (s->type == Expression::Type::Reference) {
		inflight[s->fun].reads.insert(s);
	}
	recurse_expression(ctx, s);
}



void
CaptureBlendCrossingVariables::on_lolepop(Pipeline& p, Lolepop& l)
{
#if 0
	flat = true;
	inflight.clear(),
#else
	flat = false;
#endif

	recurse_lolepop(p, l);
}

void
CaptureBlendCrossingVariables::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);

	switch (s->type) {
	case Statement::Type::Assignment:
		inflight[s->var].writes.insert(s);
		break;

	case Statement::Type::MetaStmt:
		{
			auto meta_node = (MetaStmt*)s.get();
			if (meta_node->meta_type == MetaStmt::MetaType::VarDead) {
				auto var_dead = (MetaVarDead*)meta_node;
				inflight.erase(var_dead->variable_name);
			}
			break;
		}

	case Statement::Type::Loop:
	case Statement::Type::BlendStmt:
		{
			CrossingVariables* crossing = nullptr;
			const char* op_name = nullptr;

			if (s->type == Statement::Type::Loop) {
				crossing = &(((Loop*)s.get())->crossing_variables);
				op_name = "LOOP";
			} else if (s->type == Statement::Type::BlendStmt) {
				crossing = &(((BlendStmt*)s.get())->crossing_variables);
				op_name = "BLEND";
			} else {
				ASSERT(false);
			}

			LOG_DEBUG("<%s p=%p>\n", op_name, s.get());

			// all variables seen so far
			for (const auto& var : inflight) {
				size_t read = var.second.reads.size();
				size_t write = var.second.writes.size();

				LOG_DEBUG("%p all_in %s read % d write %d\n",
					s.get(), var.first.c_str(), read, write);
				crossing->all_inputs.insert(var.first);
			}

			// find outputs generated from BLEND
			UsedVariables blend_vars;

			blend_vars.on_statement(ctx, s);

			for (const auto& var : blend_vars.inflight) {
				size_t read = var.second.reads.size();
				size_t write = var.second.writes.size();

				if (read > 0) {
					crossing->used_inputs.insert(var.first);
					LOG_DEBUG("%p used_in %s read % d write %d\n",
						s.get(), var.first.c_str(), read, write);
				}

				if (write > 0) {
					crossing->used_outputs.insert(var.first);
					crossing->all_outputs.insert(var.first);

					LOG_DEBUG("%p all/used_out %s read % d write %d\n",
						s.get(), var.first.c_str(), read, write);
				}
			}

			// add inflight outputs
			for (const auto& var : inflight) {
				size_t read = var.second.reads.size();
				size_t write = var.second.writes.size();

				LOG_DEBUG("%p all_out %s read % d write %d\n",
					s.get(), var.first.c_str(), read, write);
				crossing->all_outputs.insert(var.first);
			}

			LOG_DEBUG("</%s p=%p>\n", op_name, s.get());

			// delete outputs not covered with inputs ... i.e. local variables
			std::set<std::string> to_delete;
			for (auto& output : crossing->used_outputs) {
				if (crossing->all_inputs.find(output) != crossing->all_inputs.end()) {
					to_delete.insert(output);
				}
			}
			for (auto& del : to_delete) {
				crossing->all_outputs.erase(del);
				crossing->used_outputs.erase(del);
			}

			break;
		}

	default:
		break;
	} 

	recurse_statement(ctx, s, false);
}


void
CaptureBlendCrossingVariables::on_expression(LolepopCtx& ctx, ExprPtr& s)
{
	if (s->type == Expression::Type::Reference && s->props.refers_to_variable) {
		inflight[s->fun].reads.insert(s);
	}
	recurse_expression(ctx, s);
}

void
CaptureBlendCrossingVariables::on_program(Program& p)
{
	flat = true;
	recurse_program(p);
}


void
LimitVariableLifetimeCheck::on_program(Program& p)
{
	flat = true;
	num_fails = 0;
	trace = false;
	recurse_program(p);

	ASSERT(!num_fails);
}

void
LimitVariableLifetimeCheck::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);

	switch (s->type) {
	case Statement::Type::MetaStmt:
		{
			auto meta_node = (MetaStmt*)s.get();
			if (meta_node->meta_type == MetaStmt::MetaType::VarDead) {
				auto var_dead = (MetaVarDead*)meta_node;
				const auto& n = var_dead->variable_name;

				if (active_vars.find(n) == active_vars.end()) {
					std::cerr << "LifeTimeCheck: Trying to deallocate non-existing variable '" << n << "')" << std::endl;
					num_fails++;
				}
				active_vars.erase(n);

				if (trace) {
					std::cout << "LifeTimeCheck: Free('" << n << "')" << std::endl;
				}
			}
			break;
		}

	case Statement::Type::Assignment:
		active_vars.insert(s->var);
		all_vars.insert(s->var);
		if (trace) {
			std::cout << "LifeTimeCheck: New('" << s->var << "')" << std::endl;
		}
		break;

	default:
		break;
	}

	recurse_statement(ctx, s, false);
}

void
LimitVariableLifetimeCheck::on_expression(LolepopCtx& ctx, ExprPtr& s)
{
	if (s->type == Expression::Reference) {
		const auto& n = s->fun;

		s->props.refers_to_variable = all_vars.find(n) != all_vars.end();
		if (s->props.refers_to_variable) {
			// legit variable and not some refernce to a data structure

			if (active_vars.find(n) == active_vars.end()) {
				std::cerr << "LifeTimeCheck: Trying to access unallocated variable '" << n << "'" << std::endl;
				num_fails++;
			}
		}

	}
	recurse_expression(ctx, s);
}

void
LimitVariableLifetimeCheck::on_lolepop(Pipeline& p, Lolepop& l)
{
	ASSERT(active_vars.empty());

	all_vars.clear();
	active_vars.clear();

	recurse_lolepop(p, l);

	if (active_vars.size() > 0) {
		std::cerr << "LifeTimeCheck: Lolepop '" << l.name << "' leaks variables: " << std::endl;
		for (auto& v : active_vars) {
			std::cerr << "'" << v << "'" << std::endl;
		}
	}
}



FirstDataTouchPass::FirstDataTouchPass()
{
	flat = true;
}

void FirstDataTouchPass::on_lolepop(Pipeline& p, Lolepop& l)
{
	auto clear = [&] () {
		op_random_touches = 0;
		op_scanned.clear();
		op_scanned_attach_to.clear();
		op_scan_id = 0;
		LOG_TRACE("FirstDataTouchPass: clear @ op %s\n", l.name.c_str());
	};

	clear();
	recurse_lolepop(p, l);


	for (const auto& e : op_scanned_attach_to) {
		LOG_TRACE("FirstDataTouchPass: attached scan cols to %p\n",
			e.get());
		e->props.forw_affected_scans = op_scanned;
	}

	clear();
}


void FirstDataTouchPass::on_statement(LolepopCtx& ctx, StmtPtr& s)
{
	recurse_statement(ctx, s, true);
	recurse_statement(ctx, s, false);
}

void FirstDataTouchPass::on_expression(LolepopCtx& ctx, ExprPtr& e)
{
	recurse_expression(ctx, e);

	bool table_in = false;
	bool table_out = false;
	const auto& n = e->fun;

	if (!n.compare("scan")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		LOG_TRACE("FirstDataTouchPass: scanning tbl %s col %s op_scan_id %d\n",
			tbl.c_str(), col.c_str(), op_scan_id);
		op_scanned.push_back(e);

		if (!op_scan_id) {
			op_scanned_attach_to.push_back(e);
		}

		// e->props.first_touch_random_access = "__dummy";
		op_scan_id++;

		return;
	}

	if (!e->is_table_op(&table_out, &table_in)) {
		return;
	}
	std::string tbl, col;
	e->get_table_column_ref(tbl, col);

	bool match = false;

	if (!match && !n.compare("bucket_lookup")) {
		e->props.first_touch_random_access = "__dummy";
		match = true;
	}

	if (!match && !n.compare("bucket_next")) {
		e->props.first_touch_random_access = "next";
		match = true;
	}

	if (!match && (str_in_strings(n, {"gather", "check"}) || e->is_aggr())) {
		// FIXME: assumes NSM, modify for different layouts, see #16
		if (!op_random_touches) {
			e->props.first_touch_random_access = "col_" + col;
			ASSERT(!col.empty());
		}

		op_random_touches++;
		match = true;
	}
}