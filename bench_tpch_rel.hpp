#ifndef H_BENCH_TPCH_REL
#define H_BENCH_TPCH_REL

#include "bench.hpp"

struct QueryConfig;

BenchmarkQuery tpch_rel_q1(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q6(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q6b(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q6c(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q3(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q9(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q9a(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q9b(QueryConfig& qconf);
BenchmarkQuery tpch_rel_q9c(QueryConfig& qconf);


BenchmarkQuery tpch_rel_q18(QueryConfig& qconf);

BenchmarkQuery tpch_rel_s1(QueryConfig& qconf);
BenchmarkQuery tpch_rel_s2(QueryConfig& qconf);
BenchmarkQuery tpch_rel_j1(QueryConfig& qconf);
BenchmarkQuery tpch_rel_j2(QueryConfig& qconf);
BenchmarkQuery tpch_rel_j2a(QueryConfig& qconf);
BenchmarkQuery tpch_rel_j2b(QueryConfig& qconf);
BenchmarkQuery tpch_rel_j2c(QueryConfig& qconf);

BenchmarkQuery tpch_rel_aggr1(QueryConfig& qconf);
BenchmarkQuery tpch_rel_aggr1a(QueryConfig& qconf);

BenchmarkQuery tpch_rel_imv1(QueryConfig& qconf);

#endif