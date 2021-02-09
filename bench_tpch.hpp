#ifndef H_BENCH_TPCH
#define H_BENCH_TPCH

#include <string>
#include "bench.hpp"

namespace runtime {
	struct Database;
};
struct QueryConfig;
void setup_tpch(runtime::Database& db, int sf, const std::string& data_dir);

BenchmarkQuery prepare_tpch_query(QueryConfig& qconf, const std::string& q);

#endif