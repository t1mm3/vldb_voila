#include "utils.hpp"
#include "relalg_translator.hpp"
#include "runtime_framework.hpp"
#include <functional>

using namespace std;

void Flow::debug_print(const std::string& op)
{
	std::cerr << "<flow op='" << op << "'>" << std::endl;
	for (auto& p : col_map) {
		std::cerr << "'" << p.first << "' -> " << p.second << std::endl;
	}
	std::cerr << "</flow>" << std::endl;
}

struct ExprTranslator : relalg::RelExprVisitor {
private:
	Flow& flow;	
	ExprPtr result;
	ExprPtr lolearg;
	ExprPtr pred;

	const std::unordered_map<std::string, std::string> func_map = 
	{
		{"<=", "le"},
		{"<", "lt"},
		{">=", "ge"},
		{">", "gt"},
		{"=", "eq"},
		{"!=", "ne"},
		{"+", "add"},
		{"-", "sub"},
		{"*", "mul"},
	};

public:
	std::unordered_map<std::string, ExprPtr> expr_cache;

	ExprTranslator(Flow& f, const ExprPtr& predicate) : flow(f) {
		result = nullptr;
		lolearg = nullptr;
		pred = predicate;
	}

	ExprPtr operator()(relalg::RelExpr* e) {
		result = nullptr;
		lolearg = nullptr;
		return transl(*e);
	}

	ExprPtr operator()(std::shared_ptr<relalg::RelExpr> e) {
		result = nullptr;
		lolearg = nullptr;
		return transl(*e.get());
	}

	void visit(relalg::Const& c) final {
		result = make_shared<Const>(c.val);
	}
	
	void visit(relalg::ColId& c) final {
		const auto& id = c.id; 
		auto cit = expr_cache.find(id);

		if (cit != expr_cache.end()) {
			result = cit->second;
			return;
		}

		if (!lolearg) {
			lolearg = make_shared<LoleArg>();
		}

		auto it = flow.col_map.find(c.id);
		if (it == flow.col_map.end()) {
			cerr << "Cannot find '" << c.id <<"' inside of flow" << std::endl;
			ASSERT(it != flow.col_map.end());
		}
		result = make_shared<TupleGet>(lolearg, it->second);
		expr_cache[id] = result;
	}
	
	void visit(relalg::Fun& f) final {
		std::vector<ExprPtr> args;
		for (auto& e : f.args) {
			args.push_back(transl(*e));
		}

		std::string n = f.name;

		auto it = func_map.find(n);
		if (it != func_map.end()) {
			n = it->second;
		}

		result = make_shared<Fun>(n, args, pred);
	}

	void visit(relalg::Assign&) final {
		ASSERT(false);
	}

private:
	ExprPtr transl(relalg::RelExpr& e) {
		e.accept(*this);
		return result;
	}
};

static bool enable_all_blends(const QueryConfig& q)
{
	bool r = q.all_blends || q.full_blend;

	if (r) {
		LOG_ERROR("enable_all_blends\n");
	}
	return r;
}


template<typename T> StmtPtr
__wrap_blend(bool enable, const QueryConfig& qconfig, const T& stmts,
	const ExprPtr& pred)
{
	if (enable) {
		LOG_ERROR("test\n");
		return make_shared<BlendStmt>(stmts, pred);
	}

	return make_shared<WrapStatements>(stmts, pred);
}


template<typename T> StmtPtr
wrap_blend(bool enable, const QueryConfig& qconfig, const T& stmts, const ExprPtr& pred)
{
	return __wrap_blend<T>(enable && enable_all_blends(qconfig), qconfig, stmts, pred);
}


template<typename T> StmtPtr
wrap_blend(const QueryConfig& qconfig, const T& stmts, const ExprPtr& pred)
{
	return wrap_blend<T>(true, qconfig, stmts, pred);
}

static StmtPtr create_match_keys_no_blend(const std::string& name, const ExprPtr& check_keys,
	const ExprPtr& pred_probe_active)
{
	return make_shared<Assign>(name, check_keys, pred_probe_active);
}

static StmtPtr create_match_keys(const std::string& name, const ExprPtr& check_keys,
	const ExprPtr& pred_probe_active, const QueryConfig& config)
{
	StmtPtr match_keys_stmt = create_match_keys_no_blend(name, check_keys, pred_probe_active);

	if (enable_all_blends(config) || !config.blend_key_check.empty()) {
		auto check_blend = make_shared<BlendStmt>(StmtList {match_keys_stmt}, make_shared<LolePred>());
		check_blend->blend = make_shared<BlendConfig>(config.blend_key_check);

		match_keys_stmt = check_blend;
	}

	return match_keys_stmt;
}

RelOpTranslator::RelOpTranslator(QueryConfig& config)
 : config(config)
{
}

void
RelOpTranslator::new_pipeline()
{
	prog.pipelines.push_back(std::move(pipe));
	pipe.tag_interesting = true;
}

void
RelOpTranslator::transl_op(relalg::RelOp& op)
{
	op.accept(*this);
	flow.debug_print(op.name);
}

std::string
RelOpTranslator::lolepop_name(relalg::RelOp& op, const std::string& stage)
{
	lolepop_id_counter++;
	std::string r("lole_" + std::to_string(lolepop_id_counter) + "_" + op.name);

	if (stage.size() > 0) {
		return r + "_" + stage;
	} else {
		return r;
	}
}

void
RelOpTranslator::visit(relalg::Scan& op)
{
	auto scan_pos1 = make_shared<Ref>("pos");
	auto scan_pos = make_shared<Ref>("pos");

	std::vector<ExprPtr> col_exprs;
	std::vector<DCol> base_cols;

	const auto& table = op.table;

	auto no_pred = nullptr;

	// current columns
	flow.col_map = {};
	size_t i=0;
	for (auto& expr : op.columns) {
		ASSERT(expr && expr->type == relalg::RelExpr::Type::ColId);
		auto col = (relalg::ColId*)(expr.get());
		const auto col_name = table + "." + col->id;
		col_exprs.push_back(make_shared<Scan>(make_shared<Ref>(col_name), scan_pos, no_pred));
		flow.col_map[col_name] = i;

		base_cols.push_back(DCol(col->id, col->id));
		i++;
	};

	
	auto scan_morsel = make_shared<Ref>("morsel");
	auto scan_morsel1 = make_shared<Ref>("morsel");

	auto stmts = StmtList {
		make_shared<Assign>("morsel",
			make_shared<Fun>("scan_morsel", ExprList {
				make_shared<Ref>(table)
			},
			no_pred), no_pred),
		make_shared<Assign>("valid_morsel",
			make_shared<Fun>("selvalid", ExprList {
				scan_morsel
			},
			no_pred), no_pred),
		make_shared<Loop>(make_shared<Ref>("valid_morsel"), StmtList {
			make_shared<Assign>("pos",
				make_shared<Fun>("scan_pos", ExprList { scan_morsel1 },
				no_pred), no_pred),
			make_shared<Assign>("valid_pos",
				make_shared<Fun>("selvalid", ExprList { scan_pos1 },
				no_pred), no_pred),
			make_shared<Loop>(make_shared<Ref>("valid_pos"), StmtList {
				make_shared<Emit>(make_shared<TupleAppend>(col_exprs, no_pred), no_pred),
				make_shared<MetaRefillInflow>(),
				make_shared<Assign>("pos", make_shared<Fun>("scan_pos", ExprList { scan_morsel1 }, no_pred), no_pred),
				make_shared<Assign>("valid_pos", make_shared<Fun>("selvalid", ExprList {make_shared<Ref>("pos")}, no_pred), no_pred)
			}),
			make_shared<MetaVarDead>("pos"),
			make_shared<MetaVarDead>("valid_pos"),

			make_shared<Assign>("morsel", make_shared<Fun>("scan_morsel",
				ExprList {make_shared<Ref>(table)}, no_pred), no_pred),
			make_shared<Assign>("valid_morsel", make_shared<Fun>("selvalid",
				ExprList {make_shared<Ref>("morsel")}, no_pred), no_pred)
		}),
		make_shared<MetaVarDead>("morsel"),
		make_shared<MetaVarDead>("valid_morsel"),
		make_shared<Done>()
	};

	pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op), stmts));

	prog.data_structures.push_back(BaseTable(table, base_cols, op.table));
}

void
RelOpTranslator::visit(relalg::Project& op)
{
	transl_op(*op.left);

	std::vector<ExprPtr> cols;

	auto pred = make_shared<LolePred>();
	ExprTranslator expr_transl(flow, pred);
	Flow new_flow;

	size_t i = 0;
	for (auto& e : op.projections) {
		switch (e->type) {
		case relalg::RelExpr::Type::Assign:
			{
				auto a = (relalg::Assign*)e.get();

				new_flow.col_map[a->name] = i;

				auto expr = expr_transl(a->expr);
				expr_transl.expr_cache[a->name] = expr;
				cols.push_back(expr);


				break;
			}
		case relalg::RelExpr::Type::ColId:
			{
				auto c = (relalg::ColId*)e.get();
				new_flow.col_map[c->id] = i;
				cols.push_back(expr_transl(e));
			}
			break;
		default:
			ASSERT(false && "wrong type");
			break;
		}
		i++;
	}

	StmtList statements;

	if (enable_all_blends(config)) {
		StmtList s;

		for (size_t i=0; i<cols.size(); i++) {
			auto var = "bv_" + std::to_string(i);
			s.push_back(make_shared<Assign>(var, cols[i], pred));

			cols[i] = make_shared<Ref>(var);
		}

		statements.push_back(wrap_blend(true, config, s, pred));
	}

	statements.push_back(make_shared<Emit>(make_shared<TupleAppend>(cols, pred), pred));

	if (enable_all_blends(config)) {
		for (size_t i=0; i<cols.size(); i++) {
			statements.push_back(make_shared<MetaVarDead>("bv_" + std::to_string(i)));
		}		
	}
	pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op), statements));

	flow = new_flow;
}

void
RelOpTranslator::visit(relalg::Select& op)
{
	transl_op(*op.left);

	ExprTranslator expr_transl(flow, make_shared<LolePred>());
	ExprPtr pred = make_shared<Fun>(
		"seltrue", ExprList { expr_transl(op.predicate) },
		make_shared<LolePred>()
	);

	std::vector<ExprPtr> cols;

	auto lolearg = make_shared<LoleArg>();
	for (size_t i=0; i<flow.col_map.size(); i++) {
		cols.push_back(make_shared<TupleGet>(lolearg, i));
	}

	ASSERT(cols.size() > 0);

	StmtList statements;

	if (true) {
		StmtList s;

		auto var = "bv_pred";

		statements.push_back(wrap_blend(false, config, StmtList {
			make_shared<Assign>(var, pred, pred)
		}, pred));
		pred = make_shared<Ref>(var);
	}

	statements.push_back(make_shared<Emit>(make_shared<TupleAppend>(cols, pred), pred));
	statements.push_back(make_shared<MetaVarDead>("bv_pred"));
	pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op), statements));
}

void
RelOpTranslator::visit(relalg::HashAggr& op)
{
	transl_op(*op.left);

	std::vector<std::string> new_keys;
	std::vector<std::string> new_aggregates;

	const bool is_global_aggr = op.variant == relalg::HashAggr::Global;

	auto generate_aggregation = [&] (relalg::HashAggr& op, bool reaggr) {
		const bool flush_to_master = !reaggr;
		auto struct_name = new_unique_name("aggr_ht");

		std::vector<StmtPtr> stmts;
		std::vector<StmtPtr> aggrs;
		std::vector<std::string> key_columns;
		std::vector<std::string> aggregate_columns;

		std::vector<DCol> table_cols;

		auto statements = [&] (const std::vector<StmtPtr>& s) {
			for (auto& t : s) {
				stmts.push_back(t);
			}
		};

		const std::string group_id_str("bucket");
		ExprPtr group_id = make_shared<Ref>(group_id_str);

		size_t aggr_idx = 0;
		std::string count_colum = "";

		auto add_aggregate = [&] (auto aggr, bool visible, auto& pred, bool global) {
			const auto short_col_name = "aggr_" + std::to_string(aggr_idx);
			const auto col_name = struct_name + "." + short_col_name;
			ASSERT(aggr->type == relalg::RelExpr::Type::Fun);

			ExprTranslator transl(flow, pred);

			auto f = (relalg::Fun*)aggr.get();
			ASSERT(f);
			const auto& n = f->name;
			StmtPtr s = nullptr;
			ASSERT(f->args.size() <= 1);

			aggr_idx++;

			table_cols.push_back(DCol(short_col_name, short_col_name, DCol::Modifier::kValue));

			if (global) {
				if (!n.compare("sum")) {
					auto& arg = f->args[0];
					s = make_shared<AggrGSum>(make_shared<Ref>(col_name),
						transl(arg), pred);
				} else if (!n.compare("count")) {
					count_colum = col_name;
					s = make_shared<AggrGCount>(make_shared<Ref>(col_name),
						pred);
				} else {
					ASSERT(false && "invalid aggregate function");
				}
			} else {
				if (!n.compare("sum")) {
					auto& arg = f->args[0];
					s = make_shared<AggrSum>(make_shared<Ref>(col_name), group_id,
						transl(arg), pred);
				} else if (!n.compare("count")) {
					count_colum = col_name;
					s = make_shared<AggrCount>(make_shared<Ref>(col_name), group_id,
						group_id, pred);
				} else {
					ASSERT(false && "invalid aggregate function");
				}
			}

			if (visible) {
				aggregate_columns.push_back(col_name);
			}

			if (s) {
				aggrs.push_back(s);
			}
		};

		auto generate_aggregates = [&] (auto& pred, bool global_aggr) {
			for (auto& aggr : op.aggregates) {
				add_aggregate(aggr, true, pred, global_aggr);
			}
			if (!count_colum.size()) {
				std::shared_ptr<relalg::Fun> count(new relalg::Fun(std::string("count"), {}));
				add_aggregate(count, false, pred, global_aggr);
			}

			return aggrs;
		};

		auto lolepred = make_shared<LolePred>();

		if (is_global_aggr) { 
			group_id = 0;

			auto short_col_name = "hash_" + std::to_string(0);
			auto col_name = struct_name + "." + short_col_name;

			table_cols.push_back(DCol(short_col_name, short_col_name, DCol::Modifier::kHash));

			auto aggrs = generate_aggregates(lolepred, true);

			for (auto& a : aggrs) {
				stmts.push_back(a);
			}
		} else {
			ExprTranslator transl(flow, make_shared<Ref>("found"));

			// hash keys
			ExprPtr hash_expr = nullptr;
			ExprPtr check_expr = nullptr;
			std::vector<StmtPtr> scatter_keys;
			size_t key_idx = 0;
			ExprPtr check_index_expr = make_shared<Ref>("bucket");
			ExprPtr scatter_index_expr = make_shared<Ref>("new_pos");
			ExprPtr const0 = make_shared<Const>("0");

			ExprPtr miss_pred = make_shared<Ref>("miss");
			ExprPtr hit_pred = make_shared<Ref>("hit");
			ExprPtr hit_pred2 = make_shared<Ref>("hit");
			ExprPtr scatter_pred = make_shared<Ref>("can_scatter");

			std::vector<ExprPtr> translated_keys;

			for (auto& key : op.keys) {
				auto short_col_name = "key_" + std::to_string(key_idx);
				auto col_name = struct_name + "." + short_col_name;
				auto tk = transl(key);


				table_cols.push_back(DCol(short_col_name, short_col_name, DCol::Modifier::kKey));

				translated_keys.push_back(tk);
				key_columns.push_back(col_name);

				// hash key
				if (hash_expr) {
					hash_expr = make_shared<Fun>("rehash", ExprList {hash_expr, tk}, lolepred);
				} else {
					hash_expr = make_shared<Fun>("hash", ExprList {tk}, lolepred);
				}

				// produce key check
				ExprPtr chk = make_shared<Fun>("check", ExprList {
					make_shared<Ref>(col_name),
					check_index_expr,
					tk
				}, hit_pred);
				if (check_expr) {
					check_expr = make_shared<Fun>("and", ExprList {check_expr, chk}, hit_pred);
				} else {
					check_expr = chk;
				}

				// scatter new key value
				scatter_keys.push_back(make_shared<Scatter>(
					make_shared<Ref>(col_name),
					scatter_index_expr,
					tk,
					scatter_pred
				));
				key_idx++;
			}

			// 
			ExprPtr no_pred = nullptr;
			auto miss_pred2 = make_shared<Ref>("miss");
			auto found_pred = make_shared<Ref>("found");

			const auto aggregates = generate_aggregates(found_pred, false);

			StmtPtr compute_aggregates = make_shared<WrapStatements>(aggregates, found_pred);
			if (false && aggregates.size() > 0 && (config.all_blends || config.full_blend || !config.blend_aggregates.empty())) {
				auto blend = make_shared<BlendStmt>(aggregates, make_shared<LolePred>());
				blend->blend = make_shared<BlendConfig>(config.blend_aggregates);
				compute_aggregates = blend;
			}

			{
				auto short_col_name = "hash_" + std::to_string(key_idx);
				auto col_name = struct_name + "." + short_col_name;

				table_cols.push_back(DCol(short_col_name, short_col_name, DCol::Modifier::kHash));

				// scatter new key value
				scatter_keys.push_back(make_shared<Scatter>(
					make_shared<Ref>(col_name),
					scatter_index_expr,
					make_shared<Ref>("hash"),
					scatter_pred
				));
			}

			StmtPtr match_keys_stmt = create_match_keys_no_blend("equal", check_expr, hit_pred);

			auto outer_loop = make_shared<Loop>(make_shared<Ref>("miss"), StmtList {
				make_shared<Assign>("bucket", make_shared<Fun>("bucket_lookup", ExprList {
					make_shared<Ref>(struct_name),
					make_shared<Ref>("hash")}, miss_pred), miss_pred),
				make_shared<Assign>("empty", make_shared<Fun>("eq", ExprList {
					make_shared<Ref>("bucket"),
					make_shared<Const>("0")}, miss_pred), miss_pred),
				make_shared<Assign>("hit", make_shared<Fun>("selfalse", ExprList {make_shared<Ref>("empty")}, miss_pred), miss_pred),
				make_shared<Assign>("miss", make_shared<Fun>("seltrue", ExprList {make_shared<Ref>("empty")}, miss_pred), miss_pred),
				make_shared<MetaVarDead>("empty"),

				make_shared<Loop>(make_shared<Ref>("hit"), StmtList {
					// equal = check_expr | hit_pred
					// make_shared<Assign>("equal", check_expr, hit_pred),
					match_keys_stmt,

					make_shared<Assign>("found", make_shared<Fun>("seltrue", ExprList {make_shared<Ref>("equal")}, hit_pred), hit_pred),
					
					// make_shared<Assign>(group_id_str, make_shared<Ref>("bucket")),
					compute_aggregates,
					make_shared<MetaVarDead>("found"),

					make_shared<Assign>("hit", make_shared<Fun>("selfalse", ExprList {make_shared<Ref>("equal")}, hit_pred), hit_pred),
					make_shared<MetaVarDead>("equal"),
					make_shared<Assign>("bucket", make_shared<Fun>("bucket_next", ExprList {
						make_shared<Ref>(struct_name),
						make_shared<Ref>("bucket")
					}, hit_pred2), hit_pred2),
					make_shared<Assign>("empty", make_shared<Fun>("eq", ExprList {
						make_shared<Ref>("bucket"),
						const0
					}, hit_pred2), hit_pred2),
					make_shared<Assign>("miss", make_shared<Fun>("selunion", ExprList {
						make_shared<Ref>("miss"),
						make_shared<Fun>("seltrue", ExprList {make_shared<Ref>("empty")}, hit_pred2),
					}, no_pred), no_pred),
					make_shared<Assign>("hit", make_shared<Fun>("selfalse", ExprList {make_shared<Ref>("empty")}, hit_pred2), hit_pred2),
					make_shared<MetaVarDead>("empty"),
				}),

				make_shared<MetaVarDead>("bucket"),
				make_shared<MetaVarDead>("hit"),

				// insert miss
				make_shared<Assign>("new_pos",
					make_shared<Fun>("bucket_insert", ExprList {
						make_shared<Ref>(struct_name),
						make_shared<Ref>("hash")
					}, miss_pred2), miss_pred2),

				make_shared<Assign>("can_scatter",
					make_shared<Fun>("selfalse", ExprList {
						make_shared<Fun>("eq", ExprList {
							make_shared<Ref>("new_pos"),
							const0
						}, miss_pred2)
					}, miss_pred2), miss_pred2),


				// scatter keys |can_scatter into new_pos
				make_shared<WrapStatements>(scatter_keys, scatter_pred),

				make_shared<MetaVarDead>("new_pos"),
				make_shared<MetaVarDead>("can_scatter"),
			});

			statements(StmtList {
				make_shared<Assign>("hash", hash_expr, lolepred),
				make_shared<Assign>("miss", lolepred, lolepred),
				make_shared<MetaBeginFsmExclusive>(),
				outer_loop,
				make_shared<MetaEndFsmExclusive>(),
				make_shared<MetaVarDead>("hash"),
				make_shared<MetaVarDead>("miss"),
			});
		}

		auto table_type = DataStructure::kHashTable;
		DataStructure::Flags flags = DataStructure::kThreadLocal;
		if (flush_to_master) {
			flags |= DataStructure::kFlushToMaster;
		}

		prog.data_structures.push_back(Table(struct_name, { table_cols }, table_type,
			flags));

		pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "build"), std::move(stmts)));

		if (reaggr) {
			pipe.tag_interesting = false;
		}

		new_pipeline();

		if (flush_to_master) {
			ExprPtr lolepred_build = nullptr;

			auto statements = StmtList {
				make_shared<Effect>(make_shared<Fun>("bucket_flush", ExprList {
						make_shared<Ref>(struct_name)
					}, lolepred_build)),
				make_shared<Done>()
			};

			pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "flush"), statements));
			pipe.tag_interesting = false;

			new_pipeline();		
		}

		ExprPtr no_pred = nullptr;

		auto pos = make_shared<Ref>("pos");

		ExprPtr pred;

		ASSERT(count_colum.size() > 0);
		auto count = make_shared<Fun>("read", ExprList {make_shared<Ref>(count_colum), pos}, no_pred);
		pred = make_shared<Fun>("gt", ExprList {count, make_shared<Const>("0")}, no_pred);
		pred = make_shared<Fun>("seltrue", ExprList {pred}, no_pred);			

		auto pos2 = make_shared<Ref>("pos");

		std::vector<ExprPtr> out_cols;
		Flow new_flow;
		size_t output_col_id = 0;

		auto add_out_col = [&] (const auto& name) {
			ASSERT(name.size() > 0);
			out_cols.push_back(make_shared<Fun>("read", ExprList {make_shared<Ref>(name), pos}, no_pred));
			new_flow.col_map[name] = output_col_id;
			output_col_id++;
		};

		for (auto& col : key_columns) {
			add_out_col(col);
			new_keys.push_back(col);
		}

		for (auto& col : aggregate_columns) {
			add_out_col(col);
			new_aggregates.push_back(col);
		}


		auto scan_morsel = make_shared<Ref>("morsel");
		auto scan_morsel1 = make_shared<Ref>("morsel");

		{
			auto stmts = StmtList {
				make_shared<Assign>("morsel",
					make_shared<Fun>("read_morsel", ExprList {
						make_shared<Ref>(struct_name)
					},
					no_pred), no_pred),
				make_shared<Assign>("valid_morsel",
					make_shared<Fun>("selvalid", ExprList {
						scan_morsel
					},
					no_pred), no_pred),
				make_shared<Loop>(make_shared<Ref>("valid_morsel"), StmtList {
					make_shared<Assign>("pos",
						make_shared<Fun>("read_pos", ExprList { scan_morsel1 },
						no_pred), no_pred),
					make_shared<Assign>("valid_pos",
						make_shared<Fun>("selvalid", ExprList { pos2 },
						no_pred), no_pred),
					make_shared<Loop>(make_shared<Ref>("valid_pos"), StmtList {
						make_shared<Emit>(make_shared<TupleAppend>(out_cols, pred), pred),
						make_shared<MetaRefillInflow>(),
						make_shared<Assign>("pos", make_shared<Fun>("read_pos", ExprList { scan_morsel1 }, no_pred), no_pred),
						make_shared<Assign>("valid_pos", make_shared<Fun>("selvalid", ExprList {make_shared<Ref>("pos")}, no_pred), no_pred)
					}),

					make_shared<MetaVarDead>("pos"),
					make_shared<MetaVarDead>("valid_pos"),

					make_shared<Assign>("morsel", make_shared<Fun>("read_morsel",
						ExprList {make_shared<Ref>(struct_name)}, no_pred), no_pred),
					make_shared<Assign>("valid_morsel", make_shared<Fun>("selvalid",
						ExprList {make_shared<Ref>("morsel")}, no_pred), no_pred)
				}),
				make_shared<MetaVarDead>("morsel"),
				make_shared<MetaVarDead>("valid_morsel"),
				make_shared<Done>()
			};


			pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "read"), stmts));		
		}

		flow = new_flow;
	};

	generate_aggregation(op, false);

	// morsel-driven parallelism requires re-aggregation on partitions
	{
		// auto dummy = make_shared<Scan>("", {});
		std::vector<std::shared_ptr<relalg::RelExpr>> key_cols
			= relalg::RelExpr::from_column_names(new_keys);
		std::vector<std::shared_ptr<relalg::RelExpr>> agg_cols
			= relalg::RelExpr::from_column_names(new_aggregates);

		std::vector<std::shared_ptr<relalg::RelExpr>> aggregates;
		for (size_t i=0; i<agg_cols.size(); i++) {
			shared_ptr<relalg::RelExpr>& col = agg_cols[i];
			auto& aggr = op.aggregates[i];

			ASSERT(aggr->type == relalg::RelExpr::Type::Fun);

			auto f = (relalg::Fun*)aggr.get();
			ASSERT(f);
			auto n = f->name;

			if (!n.compare("count")) {
				n = "sum";
			}

			std::shared_ptr<relalg::Fun> c(new relalg::Fun(n, {col}));
			aggregates.push_back(c);
		}

		relalg::HashAggr reaggr(op.variant,
			nullptr, key_cols, aggregates);
		generate_aggregation(reaggr, true);
	}
}

void
RelOpTranslator::visit(relalg::HashJoin& op)
{
	std::string struct_name = new_unique_name("join_ht");

	std::vector<std::string> keys;

	std::vector<StmtPtr> statements;
	std::string hash_col;
	std::vector<std::string> right_key_map;
	ExprPtr reconstruct_pred = make_shared<Ref>("hit");
	ExprPtr right_reconstruct_index = make_shared<Ref>("bucket");
	std::vector<ExprPtr> right_reconstruct_gather;
	Flow new_flow;

	std::vector<DCol> table_cols;

	// -------------------- materialize ------------------------------------
	{
		transl_op(*op.right);

		ExprPtr lolepred_write = make_shared<LolePred>();

		ExprPtr some_tuple = lolepred_write;
		statements = {
			make_shared<Assign>("wpos",
				make_shared<Fun>("write_pos", ExprList {
					make_shared<Ref>(struct_name),
					some_tuple,
				}, lolepred_write),
				lolepred_write)
		};


		ExprTranslator right_transl(flow, lolepred_write);

		ExprPtr wpos = make_shared<Ref>("wpos");

		size_t rcol_id = 0;
		ExprPtr hash_keys;

		auto write_column = [&] (const auto& name, const int key_index) {
			const std::string tbl_col_short("col" + std::to_string(rcol_id));
			const std::string tbl_col(struct_name + "." + tbl_col_short);
			auto expr_col = right_transl(name);

			const bool is_key = key_index >= 0;

			table_cols.push_back(DCol(tbl_col_short, tbl_col_short,
				is_key ? DCol::Modifier::kKey : DCol::Modifier::kValue));

			if (is_key) {
				hash_keys = hash_keys ?
					make_shared<Fun>("rehash", ExprList { hash_keys, expr_col }, lolepred_write) :
					make_shared<Fun>("hash", ExprList { expr_col}, lolepred_write);
			}
			statements.push_back(make_shared<Write>(make_shared<Ref>(tbl_col), wpos, expr_col, lolepred_write));
			right_key_map.push_back(tbl_col);

			ExprPtr gather = make_shared<Fun>("gather",
				ExprList {make_shared<Ref>(tbl_col), right_reconstruct_index},
				reconstruct_pred);

			if (is_key) {
				right_reconstruct_gather.push_back(nullptr);
			} else {
				right_reconstruct_gather.push_back(gather);
			}

			rcol_id++;
		};

		int key_index = 0;
		for (auto& rkey : op.right_keys) {
			write_column(rkey, key_index);
			key_index++;
		}
		for (auto& rpay : op.right_payl) {
			write_column(rpay, -1);
		}

		{
			const std::string tbl_col_short("hash" + std::to_string(rcol_id));
			hash_col = struct_name + "." + tbl_col_short;

			table_cols.push_back(DCol(tbl_col_short, tbl_col_short,
				DCol::Modifier::kHash));
		}

		statements.push_back(make_shared<Write>(make_shared<Ref>(hash_col), wpos, hash_keys, lolepred_write));
		statements.push_back(make_shared<MetaVarDead>("wpos"));


		pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "materialize"), 
			StmtList { wrap_blend(true, config, statements, lolepred_write) }));
		statements.clear();		
	}

	new_pipeline();
	//
	prog.data_structures.push_back(Table{ struct_name, { table_cols },
		DataStructure::kHashTable, DataStructure::kReadAfterWrite});

	// -------------------- build HT ------------------------------------
	{
		ExprPtr lolepred_build = nullptr;

		statements = {
			make_shared<Effect>(make_shared<Fun>("bucket_build", ExprList {
					make_shared<Ref>(struct_name)
				}, lolepred_build)),
			make_shared<Done>()
		};

		pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "build"), statements));
		pipe.tag_interesting = false;
		statements.clear();
	}


	new_pipeline();

	// -------------------- probe ------------------------------------
	{
		flow = Flow(); // debug, will be overwritten
		transl_op(*op.left);

		ExprPtr lolepred_probe = make_shared<LolePred>();

		ExprTranslator left_transl(flow, lolepred_probe);

		ExprPtr hash_keys = nullptr;
		ExprPtr check_keys = nullptr;

		ExprPtr pred_probe_active = make_shared<Ref>("active");

		std::vector<ExprPtr> translated_left_keys;
		size_t i=0;
		for (auto& key_name : op.left_keys) {
			ExprPtr key = left_transl(key_name);

			translated_left_keys.push_back(key);

			ExprPtr check = make_shared<Fun>("check", ExprList {
					make_shared<Ref>(right_key_map[i]), right_reconstruct_index, key
				}, pred_probe_active);
			hash_keys = hash_keys ?
				make_shared<Fun>("rehash", ExprList {hash_keys, key}, lolepred_probe) :
				make_shared<Fun>("hash", ExprList {key}, lolepred_probe);

			check_keys = check_keys ?
				make_shared<Fun>("and", ExprList {check_keys, check}, pred_probe_active) :
				check;

			for (int k=0; k<right_reconstruct_gather.size(); k++) {
				if (!right_reconstruct_gather[k]) {
					right_reconstruct_gather[k] = key;
					break;
				}
			}
			
			i++;
		}
	
		StmtPtr match_keys_stmt = create_match_keys("match", check_keys, pred_probe_active, config);

		// input
		ExprPtr pred_probe_hit = make_shared<Ref>("hit");
		std::vector<ExprPtr> output_columns;
		auto lolearg = make_shared<LoleArg>();

		size_t output_col_id = 0;

		for (size_t i=0; i<flow.col_map.size(); i++) {
			output_columns.push_back(make_shared<TupleGet>(lolearg, i));
			output_col_id++;
		}

		new_flow = flow;
		auto flow_rcol = [&] (auto& expr) {
			ASSERT(expr->type == relalg::RelExpr::Type::ColId);
			const auto c = (relalg::ColId*)expr.get();
			const std::string id(c->id);

			new_flow.col_map[id] = output_col_id;
			output_col_id++;
		};

		for (const auto& c : op.right_keys) {
			flow_rcol(c);
		}

		for (const auto& c : op.right_payl) {
			flow_rcol(c);
		}

		StmtPtr reconstruct_post_emit = make_shared<WrapStatements>(StmtList {}, pred_probe_hit);
		StmtPtr reconstruct_blend = make_shared<WrapStatements>(StmtList {}, pred_probe_hit);

		if (enable_all_blends(config)) {
			int i=0;
			for (auto& e : right_reconstruct_gather) {
				auto var = "bv_" + std::to_string(i);
				reconstruct_blend->statements.push_back(make_shared<Assign>(var, e, pred_probe_hit));

				output_columns.push_back(make_shared<Ref>(var));
				reconstruct_post_emit->statements.push_back(make_shared<MetaVarDead>(var));
				i++;
			}
			reconstruct_blend = wrap_blend(true, config, reconstruct_blend->statements,
				pred_probe_hit);
		} else {
			for (auto& e : right_reconstruct_gather) {
				output_columns.push_back(e);
			}
		}


		StmtList single_match;
		ExprPtr pred_probe_active2 = pred_probe_active;

		if (op.variant == relalg::HashJoin::Variant::Join01) {
			single_match = StmtList {
				make_shared<Assign>("active",
					make_shared<Fun>("selfalse", ExprList {
						make_shared<Ref>("match")
					}, pred_probe_active), pred_probe_active
				)
			};
			// single match optimization modifes 'active'
			pred_probe_active2 = make_shared<Ref>("active");
		}

		statements = StmtList {
			wrap_blend(true, config, StmtList {
				make_shared<Assign>("bucket",
					make_shared<Fun>("bucket_lookup", ExprList {
						make_shared<Ref>(struct_name),
						hash_keys
					}, lolepred_probe),
					lolepred_probe
					),
				make_shared<Assign>("active",
					make_shared<Fun>("selfalse", ExprList {
						make_shared<Fun>("eq", ExprList {
							make_shared<Const>("0"),
							make_shared<Ref>("bucket")
						}, lolepred_probe)
					}, lolepred_probe),
					lolepred_probe
				)
			}, lolepred_probe),

			make_shared<Loop>(make_shared<Ref>("active"), StmtList {
				// 'match' = check_keys | pred_probe_active
				// make_shared<Assign>("match", check_keys, pred_probe_active),
				match_keys_stmt,
				make_shared<Assign>("hit",
					make_shared<Fun>("seltrue", ExprList {
						make_shared<Ref>("match")
					}, pred_probe_active), pred_probe_active),


				reconstruct_blend,

				// make_shared<Emit>()
				make_shared<Emit>(
					make_shared<TupleAppend>(output_columns, pred_probe_hit),
					pred_probe_hit),

				reconstruct_post_emit,

				make_shared<MetaVarDead>("hit"),

				// add single match optimization
				make_shared<WrapStatements>(single_match, pred_probe_active),
				make_shared<MetaVarDead>("match"),

				make_shared<Assign>("bucket",
					make_shared<Fun>("bucket_next", ExprList {
						make_shared<Ref>(struct_name),
						make_shared<Ref>("bucket")
					}, pred_probe_active2
				), pred_probe_active2),
				make_shared<Assign>("active",
					make_shared<Fun>("selfalse", ExprList {
						make_shared<Fun>("eq", ExprList {
							make_shared<Ref>("bucket"),
							make_shared<Const>("0")
						}, pred_probe_active2)
					}, pred_probe_active2),
					pred_probe_active2
				)
			}),
			make_shared<MetaVarDead>("active"),
			make_shared<MetaVarDead>("bucket"),
		};

		pipe.lolepops.push_back(make_shared<Lolepop>(lolepop_name(op, "probe"),
			statements));
	}
	statements.clear();

	flow = new_flow;
}


void
RelOpTranslator::operator()(relalg::RelOp& op)
{
	transl_op(op);

	new_pipeline();
}