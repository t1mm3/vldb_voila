#include "bench_tpch_rel.hpp"

#include "runtime_framework.hpp"
#include "relalg.hpp"
#include "bench.hpp"
#include "common/runtime/Import.hpp"
#include "common/runtime/Types.hpp"

using namespace relalg;
using namespace std;

#define expr_vec_t std::vector<std::shared_ptr<RelExpr>>

static void
add_num_tuples(QueryConfig& cfg, const std::vector<std::string>& tables)
{
	size_t n = 0;
	for (auto& table : tables) {
		n += cfg.db[table].nrTuples;
	}

   	cfg.num_tuples = n;
}


BenchmarkQuery
tpch_rel_q1(QueryConfig& qconf)
{
	auto scan = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_shipdate", "l_returnflag", "l_linestatus",
		"l_extendedprice", "l_quantity", "l_discount",
		"l_tax"
	}));

	auto select = make_shared<Select>(scan, make_shared<Fun>("<=", expr_vec_t {
		make_shared<ColId>("lineitem.l_shipdate"),
		make_shared<Const>(std::to_string(types::Date::castString("1998-09-02").value))
	}));

	auto one = std::to_string(types::Numeric<12, 2>::castString("1.00").value);

	auto project = make_shared<Project>(select, expr_vec_t {
		make_shared<Assign>("_TRSDM_6", make_shared<Fun>("-", expr_vec_t {make_shared<Const>(one), make_shared<ColId>("lineitem.l_discount")})),
		make_shared<Assign>("_TRSDM_7", make_shared<Fun>("*", expr_vec_t {make_shared<ColId>("_TRSDM_6"), make_shared<ColId>("lineitem.l_extendedprice")})),
		make_shared<Assign>("_TRSDM_8",
			make_shared<Fun>("*", expr_vec_t {
				make_shared<Fun>("*", expr_vec_t {
					make_shared<Fun>("+", expr_vec_t {
						make_shared<Const>(one),
						make_shared<ColId>("lineitem.l_tax")
					}),
					make_shared<ColId>("_TRSDM_6")
				}),
				make_shared<ColId>("lineitem.l_extendedprice")
			}
		)),
		make_shared<ColId>("lineitem.l_quantity"),
		make_shared<ColId>("lineitem.l_discount"),
		make_shared<ColId>("lineitem.l_extendedprice"),
		make_shared<ColId>("lineitem.l_returnflag"),
		make_shared<ColId>("lineitem.l_linestatus")
	});

	auto aggr = make_shared<HashAggr>(
		HashAggr::Variant::Hash,
		project,
		RelExpr::from_column_names({"lineitem.l_returnflag", "lineitem.l_linestatus"}),
		expr_vec_t {
			make_shared<Fun>("count", expr_vec_t {}),
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("lineitem.l_quantity")}),
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("lineitem.l_extendedprice")}),
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("_TRSDM_7")}),
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("_TRSDM_8")}),
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("lineitem.l_discount")})
		}
	);

	add_num_tuples(qconf, {"lineitem"});
	BenchmarkQuery query;
	query.root = aggr;

	query.expensive_pipelines[0] = 100;
	return query;
}



static BenchmarkQuery
_tpch_rel_q6(QueryConfig& qconf, int flavor)
{
	auto c1 = types::Date::castString("1994-01-01").value;
	auto c2 = types::Date::castString("1995-01-01").value;

	auto c3 = types::Numeric<12, 2>::castString("0.05").value;
	auto c4 = types::Numeric<12, 2>::castString("0.07").value;

	// auto c5 = types::Integer(24).value;
	auto c5 = types::Numeric<15, 2>::castString("24").value;

	auto scan = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_shipdate", "l_extendedprice", "l_quantity", "l_discount"
	}));


	std::shared_ptr<Select> select;

	auto ge_shipdate = make_shared<Fun>(">=", expr_vec_t { make_shared<ColId>("lineitem.l_shipdate"), make_shared<Const>(c1) });
	auto lt_shipdate = make_shared<Fun>("<", expr_vec_t { make_shared<ColId>("lineitem.l_shipdate"), make_shared<Const>(c2) });
	auto lt_quantity = make_shared<Fun>("<", expr_vec_t { make_shared<ColId>("lineitem.l_quantity"), make_shared<Const>(c5) });
	auto ge_discount = make_shared<Fun>(">=", expr_vec_t { make_shared<ColId>("lineitem.l_discount"), make_shared<Const>(c3) });
	auto lt_discount = make_shared<Fun>("<=", expr_vec_t { make_shared<ColId>("lineitem.l_discount"), make_shared<Const>(c4) });

	switch (flavor) {
	case 0:
		select = make_shared<Select>(scan, Fun::create_left_deep_tree("and", expr_vec_t {
			ge_shipdate, lt_shipdate,
			lt_quantity,
			ge_discount, lt_discount
		}));
		break;

	case 1:
		// TimoKersten:
		// <(shipdate)
		// >(shipdate)
		// <(_l_quantity, 24)
		// >(discount)
		// <(discount)

		select = make_shared<Select>(scan, lt_shipdate);
		select = make_shared<Select>(select, ge_shipdate);
		select = make_shared<Select>(select, lt_quantity);
		select = make_shared<Select>(select, ge_discount);
		select = make_shared<Select>(select, lt_discount);
		break;

	case 2:
		// Boncz:
		// <(shipdate, 1995-01-01)
		// >=(discount, 0.05)
		// <(_l_quantity, 24)
		// >=(_l_shipdate)
		// <=(_l_discount)

		select = make_shared<Select>(scan, lt_shipdate);
		select = make_shared<Select>(select, ge_discount);
		select = make_shared<Select>(select, lt_quantity);
		select = make_shared<Select>(select, ge_shipdate);
		select = make_shared<Select>(select, lt_discount);
		break;
	}

	auto project = make_shared<Project>(select, expr_vec_t {
		make_shared<Assign>("revenue", make_shared<Fun>("*", expr_vec_t {make_shared<ColId>("lineitem.l_extendedprice"), make_shared<ColId>("lineitem.l_discount")})),
	});

	auto aggr = make_shared<HashAggr>(
		HashAggr::Variant::Global,
		project,
		expr_vec_t {},
		expr_vec_t {
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("revenue")})
		}
	);

	add_num_tuples(qconf, {"lineitem"});

	BenchmarkQuery query;

	query.root = aggr;
	query.expensive_pipelines[0] = 100;
	return query;
}


BenchmarkQuery
tpch_rel_q6(QueryConfig& qconf)
{
	return _tpch_rel_q6(qconf, 1);
}


BenchmarkQuery
tpch_rel_q6b(QueryConfig& qconf)
{
	return _tpch_rel_q6(qconf, 0);
}

BenchmarkQuery
tpch_rel_q6c(QueryConfig& qconf)
{
	return _tpch_rel_q6(qconf, 2);
}

static BenchmarkQuery
__tpch_rel_aggr1(QueryConfig& qconf, const std::string& filter)
{
	shared_ptr<RelOp> lineitem = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_shipdate"
	}));

	if (filter.size() > 0) {
		lineitem = make_shared<Select>(lineitem, make_shared<Fun>("gt", expr_vec_t {
			make_shared<ColId>("lineitem.l_shipdate"),
			make_shared<Const>(std::to_string(types::Date::castString(filter).value))
		}));
	}

	auto aggr = make_shared<HashAggr>(HashAggr::Variant::Hash,
		lineitem,
		RelExpr::from_column_names({"lineitem.l_shipdate"}),
		expr_vec_t {}
	);


	BenchmarkQuery query;

	query.root = aggr;
	return query;
}

BenchmarkQuery
tpch_rel_aggr1(QueryConfig& qconf)
{
	return __tpch_rel_aggr1(qconf, "");
}

BenchmarkQuery
tpch_rel_aggr1a(QueryConfig& qconf)
{
	return __tpch_rel_aggr1(qconf, "1995-03-15");
}

BenchmarkQuery
tpch_rel_q3(QueryConfig& qconf)
{
	const auto c1 = std::to_string(types::Date::castString("1995-03-15").value);
	const auto c2 = std::to_string(types::Date::castString("1995-03-15").value);
	const auto one = std::to_string(types::Numeric<12, 2>::castString("1.00").value);
	const auto zero = std::to_string(types::Numeric<12, 4>::castString("0.00").value);

	auto _customer = make_shared<Scan>("customer", RelExpr::from_column_names({
		"c_mktsegment", "c_custkey"
	}));

	auto customer = make_shared<Select>(_customer, make_shared<Fun>("eq", expr_vec_t {
		make_shared<ColId>("customer.c_mktsegment"),
		make_shared<Const>("BUILDING")
	}));

	auto _orders = make_shared<Scan>("orders", RelExpr::from_column_names({
		"o_custkey", "o_orderkey", "o_orderdate", "o_shippriority"
	}));

	auto orders = make_shared<Select>(_orders, make_shared<Fun>("lt", expr_vec_t {
		make_shared<ColId>("orders.o_orderdate"),
		make_shared<Const>(c1)
	}));

	auto customerorders = make_shared<HashJoin>(HashJoin::Variant::Join01,
		orders,
		RelExpr::from_column_names({"orders.o_custkey"}),
		RelExpr::from_column_names({"orders.o_orderdate", "orders.o_shippriority",
			"orders.o_orderkey"}),

		customer,
		RelExpr::from_column_names({"customer.c_custkey"}),
		RelExpr::from_column_names({})
	);

	auto _lineitem = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"
	}));

	auto lineitem = make_shared<Select>(_lineitem, make_shared<Fun>("gt", expr_vec_t {
		make_shared<ColId>("lineitem.l_shipdate"),
		make_shared<Const>(c2)
	}));


	auto lineitemcustomerorders = make_shared<HashJoin>(HashJoin::Variant::Join01,
		lineitem,
		RelExpr::from_column_names({"lineitem.l_orderkey"}),
		RelExpr::from_column_names({"lineitem.l_extendedprice", "lineitem.l_discount"}),

		customerorders,
		RelExpr::from_column_names({"orders.o_orderkey"}),
		RelExpr::from_column_names({"orders.o_orderdate", "orders.o_shippriority"})
	);

	auto project = make_shared<Project>(lineitemcustomerorders,
		expr_vec_t {
			make_shared<Assign>("revenue", 
				make_shared<Fun>("*", expr_vec_t {
					make_shared<ColId>("lineitem.l_extendedprice"),
					make_shared<Fun>("-", expr_vec_t {make_shared<Const>(one),
						make_shared<ColId>("lineitem.l_discount")})
				})
			),
			RelExpr::from_column_name("lineitem.l_orderkey"),
			RelExpr::from_column_name("orders.o_orderdate"),
			RelExpr::from_column_name("orders.o_shippriority")
		}
	);

	auto aggr = make_shared<HashAggr>(HashAggr::Variant::Hash,
		project,
		RelExpr::from_column_names({"lineitem.l_orderkey",
			"orders.o_orderdate", "orders.o_shippriority"}),
		expr_vec_t {
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("revenue")}),
		}
	);

	add_num_tuples(qconf, {"customer", "orders", "lineitem"});

	BenchmarkQuery query;

	query.root = aggr;
	query.expensive_pipelines[4] = 70;
	query.expensive_pipelines[2] = 30;
	return query;
}


static BenchmarkQuery
__tpch_rel_q9(QueryConfig& qconf, int until6)
{
	auto nation = make_shared<Scan>("nation", RelExpr::from_column_names({
		"n_nationkey", "n_name"
	}));

	auto supplier = make_shared<Scan>("supplier", RelExpr::from_column_names({
		"s_nationkey", "s_suppkey"
	}));

	std::shared_ptr<RelOp> nationsupp = make_shared<HashJoin>(HashJoin::Variant::Join01,
		supplier,
		RelExpr::from_column_names({"supplier.s_nationkey"}),
		RelExpr::from_column_names({"supplier.s_suppkey"}),

		nation,
		RelExpr::from_column_names({"nation.n_nationkey"}),
		RelExpr::from_column_names({"nation.n_name"})
	);

	if (until6 == 2 || until6 == 1) {	
		nationsupp = make_shared<Select>(nationsupp, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("nation.n_nationkey"),
			make_shared<Const>("10")
		}));

		nationsupp = make_shared<Select>(nationsupp, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("supplier.s_suppkey"),
			make_shared<Const>("100")
		}));

	}
	auto part = make_shared<Scan>("part", RelExpr::from_column_names({
		"p_partkey", "p_name"
	}));

	// like %green% via memmem?
	std::shared_ptr<RelOp> part_green = make_shared<Select>(part, make_shared<Fun>("contains", expr_vec_t {
		make_shared<ColId>("part.p_name"),
		make_shared<Const>("green")
	}));

	std::shared_ptr<RelOp> partsupp = make_shared<Scan>("partsupp", RelExpr::from_column_names({
		"ps_partkey", "ps_suppkey", "ps_supplycost"
	}));

	if (until6 == 2 || until6 == 1) {
		part_green = make_shared<Select>(part_green, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("part.p_partkey"),
			make_shared<Const>("10000")
		}));

		partsupp = make_shared<Select>(partsupp, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("partsupp.ps_partkey"),
			make_shared<Const>("10000")
		}));
	}

	auto partpartsupp = make_shared<HashJoin>(HashJoin::Variant::Join01,
		partsupp,
		RelExpr::from_column_names({"partsupp.ps_partkey"}),
		RelExpr::from_column_names({"partsupp.ps_suppkey", "partsupp.ps_supplycost"}),

		part_green,
		RelExpr::from_column_names({"part.p_partkey"}),
		expr_vec_t {}
	);

	if (until6 == 1) {
		add_num_tuples(qconf, {"part", "partsupp"});
		BenchmarkQuery query;

		query.root = partpartsupp;
		return query;	
	}
	auto pspp = make_shared<HashJoin>(HashJoin::Variant::Join01,
		partpartsupp,
		RelExpr::from_column_names({"partsupp.ps_suppkey"}),
		RelExpr::from_column_names({"partsupp.ps_partkey", "partsupp.ps_supplycost"}),

		nationsupp,
		RelExpr::from_column_names({"supplier.s_suppkey"}),
		RelExpr::from_column_names({"nation.n_name"})
	);

	if (until6 == 2) {
		add_num_tuples(qconf, {"nation", "supplier", "part", "partsupp"});
		BenchmarkQuery query;

		query.root = pspp;
		return query;	
	}

	if (until6 == 3) {
		add_num_tuples(qconf, {"nation", "supplier"});
		BenchmarkQuery query;

		query.root = nationsupp;
		return query;	
	}

	auto lineitem = make_shared<Scan>("lineitem",
		RelExpr::from_column_names({"l_partkey", "l_suppkey", "l_orderkey",
			"l_quantity", "l_extendedprice", "l_discount"}));

	auto lineitem_join = make_shared<HashJoin>(HashJoin::Variant::Join01,
		lineitem,
		RelExpr::from_column_names({"lineitem.l_partkey", "lineitem.l_suppkey"}),
		RelExpr::from_column_names({"lineitem.l_orderkey", "lineitem.l_quantity",
			"lineitem.l_extendedprice", "lineitem.l_discount"}),

		pspp,
		RelExpr::from_column_names({"partsupp.ps_partkey", "partsupp.ps_suppkey"}),
		RelExpr::from_column_names({"nation.n_name", "partsupp.ps_supplycost"})
	);

	auto orders = make_shared<Scan>("orders",
		RelExpr::from_column_names({"o_orderkey", "o_orderdate"}));

	auto lineorders = make_shared<HashJoin>(HashJoin::Variant::JoinN, // cannot prove the absence, maybe ask Peter
		// weird we build the HT for lineitem?
		orders,
		RelExpr::from_column_names({"orders.o_orderkey"}),
		RelExpr::from_column_names({"orders.o_orderdate"}),

		lineitem_join,
		RelExpr::from_column_names({"lineitem.l_orderkey"}),
		RelExpr::from_column_names({"lineitem.l_extendedprice",
			"lineitem.l_discount", "lineitem.l_quantity",
			"partsupp.ps_supplycost", "nation.n_name"})
	);

	auto one = std::to_string(types::Numeric<12, 2>::castString("1.00").value);

	auto lineproject = make_shared<Project>(lineorders,
		expr_vec_t {
			make_shared<Assign>("amount", 
				make_shared<Fun>("-", expr_vec_t {
					make_shared<Fun>("*", expr_vec_t {
						make_shared<ColId>("lineitem.l_extendedprice"),
						make_shared<Fun>("-", expr_vec_t {make_shared<Const>(one),
							make_shared<ColId>("lineitem.l_discount")})
					}),
					make_shared<Fun>("*", expr_vec_t {
						make_shared<ColId>("partsupp.ps_supplycost"),
						make_shared<ColId>("lineitem.l_quantity"),
					})
				})
			),
			make_shared<Assign>("o_year", 
				make_shared<Fun>("extract_year", expr_vec_t {
					make_shared<ColId>("orders.o_orderdate"),
				})
			),
			RelExpr::from_column_name("nation.n_name"),
		}
	);

	auto aggr = make_shared<HashAggr>(HashAggr::Variant::Hash,
		lineproject,
		RelExpr::from_column_names({"nation.n_name", "o_year"}),
		expr_vec_t {
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("amount")}),
		}
	);

	add_num_tuples(qconf, {"nation", "supplier", "part", "partsupp", "lineitem", "orders"});
	BenchmarkQuery query;

	query.root = aggr;

	query.expensive_pipelines[8] = 45;
	query.expensive_pipelines[10] = 45;
	// query.expensive_pipelines[6] = 10;
	return query;
}


BenchmarkQuery
tpch_rel_q9(QueryConfig& qconf)
{
	return __tpch_rel_q9(qconf, 0);
}


BenchmarkQuery
tpch_rel_q9a(QueryConfig& qconf)
{
	return __tpch_rel_q9(qconf, 1);
}

BenchmarkQuery
tpch_rel_q9b(QueryConfig& qconf)
{
	return __tpch_rel_q9(qconf, 2);
}

BenchmarkQuery
tpch_rel_q9c(QueryConfig& qconf)
{
	return __tpch_rel_q9(qconf, 3);
}


static BenchmarkQuery
__tpch_rel_q18(QueryConfig& qconf, int modifier)
{
	auto lineitem1 = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_orderkey", "l_quantity"
	}));

	std::shared_ptr<RelOp> lineitem1_grouped = make_shared<HashAggr>(HashAggr::Variant::Hash,
		lineitem1,
		RelExpr::from_column_names({"lineitem.l_orderkey"}),
		expr_vec_t {
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("lineitem.l_quantity")}),
		}
	);

	const std::string table("aggr_ht4");

	lineitem1_grouped = make_shared<Project>(lineitem1_grouped, expr_vec_t {
		make_shared<Assign>("ag_orderkey",
			make_shared<ColId>(table + ".key_0")),
		make_shared<Assign>("ag_quantity",
			make_shared<ColId>(table + ".aggr_0")),
	});

	auto three_hundred = std::to_string(types::Numeric<12, 2>::castString("300").value);

	lineitem1_grouped = make_shared<Select>(lineitem1_grouped, make_shared<Fun>(">", expr_vec_t {
		make_shared<ColId>("ag_quantity"),
		make_shared<Const>(three_hundred)
	}));

	if (modifier == 1) {
		add_num_tuples(qconf, {"lineitem"});
		BenchmarkQuery query;

		query.root = lineitem1_grouped;
		return query;
	}

	auto orders = make_shared<Scan>("orders", RelExpr::from_column_names({
		"o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"
	}));

	// semi join 'lineitem1_selected' with 'orders' ie. filter orders with litem
	auto subquery_orders = make_shared<HashJoin>(HashJoin::Variant::JoinN, // FIXME: not sure, trial and error :)
		orders,
		RelExpr::from_column_names({"orders.o_orderkey"}),
		RelExpr::from_column_names({"orders.o_custkey", "orders.o_orderdate",
			"orders.o_totalprice"}),

		lineitem1_grouped,
		RelExpr::from_column_names({"ag_orderkey"}),
		RelExpr::from_column_names({})
	);


	auto customer = make_shared<Scan>("customer", RelExpr::from_column_names({
		"c_custkey", "c_name"
	}));

	// join on 'custkey' get c_name
	auto custjoin = make_shared<HashJoin>(HashJoin::Variant::JoinN, // FIXME: not sure, trial and error :)
		subquery_orders,
		RelExpr::from_column_names({"orders.o_custkey"}),
		RelExpr::from_column_names({}),

		customer,
		RelExpr::from_column_names({"customer.c_custkey"}),
		RelExpr::from_column_names({"customer.c_name"})
	);

	auto lineitem2 = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_orderkey", "l_quantity"
	}));

	// join lineitem
	auto linejoin = make_shared<HashJoin>(HashJoin::Variant::JoinN, // FIXME: not sure, trial and error :)
		lineitem2,
		RelExpr::from_column_names({"lineitem.l_orderkey"}),
		RelExpr::from_column_names({}),

		custjoin,
		RelExpr::from_column_names({"orders.o_orderkey"}),
		RelExpr::from_column_names({"orders.o_custkey", "orders.o_orderdate",
			"orders.o_totalprice", "customer.c_name"})
	);

	//
	auto aggr = make_shared<HashAggr>(HashAggr::Variant::Hash,
		linejoin,
		RelExpr::from_column_names({"customer.c_name", "orders.o_custkey",
			"orders.o_orderdate", "orders.o_totalprice",
			"lineitem.l_orderkey" // ,"l_quantity" // derived
		}),
		expr_vec_t {
			make_shared<Fun>("sum", expr_vec_t {make_shared<ColId>("lineitem.l_quantity")}),
		}
	);

	add_num_tuples(qconf, {"lineitem", "orders", "customer", "lineitem"});
	BenchmarkQuery query;

	ASSERT(false && "needs annotating");

	query.root = aggr;
	return query;
}



BenchmarkQuery
tpch_rel_q18a(QueryConfig& qconf)
{
	return __tpch_rel_q18(qconf, 1);
}

BenchmarkQuery
tpch_rel_q18(QueryConfig& qconf)
{
	return __tpch_rel_q18(qconf, 0);
}


BenchmarkQuery
tpch_rel_s1(QueryConfig& qconf)
{
	auto region = make_shared<Scan>("region", RelExpr::from_column_names({
		"r_regionkey", "r_name"
	}));

	BenchmarkQuery query;
	query.root = region;
	return query;
}


BenchmarkQuery
tpch_rel_s2(QueryConfig& qconf)
{
	auto _customer = make_shared<Scan>("customer", RelExpr::from_column_names({
		"c_custkey", "c_name", "c_mktsegment"
	}));

	auto customer = make_shared<Select>(_customer, make_shared<Fun>("eq", expr_vec_t {
		make_shared<ColId>("customer.c_mktsegment"),
		make_shared<Const>("BUILDING")
	}));

	BenchmarkQuery query;
	query.root = customer;
	return query;
}

static BenchmarkQuery
__tpch_rel_j1(QueryConfig& qconf, const std::string& n_name,
	const std::string& r_name)
{
	shared_ptr<RelOp> nation = make_shared<Scan>("nation", RelExpr::from_column_names({
		"n_regionkey", "n_nationkey", "n_name"
	}));

	if (n_name.size() > 0) {
		nation = make_shared<Select>(nation, make_shared<Fun>("eq", expr_vec_t {
			make_shared<ColId>("nation.n_name"),
			make_shared<Const>(n_name)
		}));
	}

	shared_ptr<RelOp> region = make_shared<Scan>("region", RelExpr::from_column_names({
		"r_regionkey", "r_name"
	}));

	if (r_name.size() > 0) {
		region = make_shared<Select>(region, make_shared<Fun>("eq", expr_vec_t {
			make_shared<ColId>("region.r_name"),
			make_shared<Const>(r_name)
		}));
	}

	shared_ptr<RelOp> nationregion = make_shared<HashJoin>(HashJoin::Variant::Join01,
			nation,
			RelExpr::from_column_names({"nation.n_regionkey"}),
			RelExpr::from_column_names({"nation.n_name", "nation.n_nationkey"}),

			region,
			RelExpr::from_column_names({"region.r_regionkey"}),
			RelExpr::from_column_names({"region.r_name"})
		);

	BenchmarkQuery query;
	query.root = nationregion;
	return query;
}

BenchmarkQuery
tpch_rel_j1(QueryConfig& qconf)
{
	return __tpch_rel_j1(qconf, "", "");
}

static BenchmarkQuery
__tpch_rel_j2(QueryConfig& qconf, int type)
{
	std::string n_name;
	std::string r_name;


	BenchmarkQuery j1(__tpch_rel_j1(qconf, n_name, r_name));

	shared_ptr<RelOp> supplier = make_shared<Scan>("supplier", RelExpr::from_column_names({
		"s_nationkey", "s_suppkey"
	}));
	switch (type) {
	case 0:
		break;

	case 1:
		j1.root = make_shared<Select>(j1.root, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("nation.n_nationkey"),
			make_shared<Const>("10")
		}));
		break;

	case 2:
		j1.root = make_shared<Select>(j1.root, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("nation.n_nationkey"),
			make_shared<Const>("10")
		}));

		supplier = make_shared<Select>(supplier, make_shared<Fun>("<=", expr_vec_t {
			make_shared<ColId>("supplier.s_suppkey"),
			make_shared<Const>("100")
		}));
		break;

	case 3:
		{
			j1.root = make_shared<Select>(j1.root, make_shared<Fun>("<=", expr_vec_t {
				make_shared<ColId>("nation.n_nationkey"),
				make_shared<Const>("10")
			}));

			supplier = make_shared<Select>(supplier, make_shared<Fun>("<=", expr_vec_t {
				make_shared<ColId>("supplier.s_suppkey"),
				make_shared<Const>("100")
			}));

			shared_ptr<RelOp> join = make_shared<HashJoin>(HashJoin::Variant::JoinN,
				supplier,
				RelExpr::from_column_names({"supplier.s_nationkey"}),
				RelExpr::from_column_names({"supplier.s_suppkey"}),

				j1.root,
				RelExpr::from_column_names({"nation.n_nationkey"}),
				RelExpr::from_column_names({"nation.n_name", "region.r_name"})
			);

			BenchmarkQuery query;
			query.root = join;
			return query;
		}

	default:
		ASSERT(false);
		break;
	}

	shared_ptr<RelOp> join = make_shared<HashJoin>(HashJoin::Variant::JoinN,
			j1.root,
			RelExpr::from_column_names({"nation.n_nationkey"}),
			RelExpr::from_column_names({"nation.n_name", "region.r_name"}),

			supplier,
			RelExpr::from_column_names({"supplier.s_nationkey"}),
			RelExpr::from_column_names({"supplier.s_suppkey"})
		);

	BenchmarkQuery query;
	query.root = join;
	return query;
}

BenchmarkQuery tpch_rel_j2(QueryConfig& qconf) { return __tpch_rel_j2(qconf, 0); }
BenchmarkQuery tpch_rel_j2a(QueryConfig& qconf) { return __tpch_rel_j2(qconf, 1); }
BenchmarkQuery tpch_rel_j2b(QueryConfig& qconf) { return __tpch_rel_j2(qconf, 2); }
BenchmarkQuery tpch_rel_j2c(QueryConfig& qconf) { return __tpch_rel_j2(qconf, 3); }

/*

auto constrant_o_orderdate = types::Date::castString("1996-01-01");
CONSTRANT_L_QUAN 50
auto constrant_l_quantity = types::Numeric<12, 2>(types::Integer(CONSTRANT_L_QUAN));


orders (o_orderkey, o_orderdate) -> orderdate < contraint_date -> HT key = o_orderkey

lineitem (l_orderkey, l_quantity) -> l_quantity_col<constrant_l_quantity -> HT probe l_orderkey -> count(*)
*/

/*
select
        count(*)
from
        lineitem,
        orders
where
        o_orderdate < date '1996-01-01'
        and l_quantity < 50
        and l_orderkey = o_orderkey
*/

BenchmarkQuery
tpch_rel_imv1(QueryConfig& qconf)
{
	shared_ptr<RelOp> orders = make_shared<Scan>("orders", RelExpr::from_column_names({
		"o_orderkey", "o_orderdate"
	}));

	auto constraint_date = types::Date::castString("1996-01-01").value;

	orders = make_shared<Select>(orders, make_shared<Fun>("lt", expr_vec_t {
		make_shared<ColId>("orders.o_orderdate"),
		make_shared<Const>(constraint_date)
	}));

	shared_ptr<RelOp> lineitem = make_shared<Scan>("lineitem", RelExpr::from_column_names({
		"l_orderkey", "l_quantity"
	}));

	auto constraint_quant = types::Numeric<15, 2>::castString("50").value;

	lineitem = make_shared<Select>(lineitem, make_shared<Fun>("lt", expr_vec_t {
		make_shared<ColId>("lineitem.l_quantity"),
		make_shared<Const>(constraint_quant)
	}));

	shared_ptr<RelOp> join = make_shared<HashJoin>(HashJoin::Variant::Join01,
		lineitem,
		RelExpr::from_column_names({"lineitem.l_orderkey"}),
		RelExpr::from_column_names({}),

		orders,
		RelExpr::from_column_names({"orders.o_orderkey"}),
		RelExpr::from_column_names({})
	);

	auto aggr = make_shared<HashAggr>(
		HashAggr::Variant::Global,
		join,
		expr_vec_t {},
		expr_vec_t {
			make_shared<Fun>("count", expr_vec_t {make_shared<ColId>("count")})
		}
	);

	add_num_tuples(qconf, {"lineitem", "orders"});

	BenchmarkQuery query;
	query.root = aggr;
	return query;
}