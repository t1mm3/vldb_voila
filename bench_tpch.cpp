#include "bench_tpch.hpp"
#include "runtime.hpp"
#include "common/runtime/Import.hpp"
#include "common/runtime/Types.hpp"
#include "runtime_framework.hpp"
#include <sstream>

void
setup_tpch(runtime::Database& db, int sf, const std::string& data_dir)
{
	std::ostringstream dirs;
	dirs << data_dir << "/" << "tpch_" << sf;
	auto dir = dirs.str();

	std::cerr << "Setting up TPC-H SF=" << sf << " in '" << dir << "'" << std::endl;

	std::ostringstream cmd;
	cmd << "SF=" << sf << " DIRECTORY=" << dir << " ./generate_tpch.sh " << sf;
	system(cmd.str().c_str());


	// setup database
	std::cerr << "Importing TPC-H" << std::endl;
	importTPCH(dir, db);
}

#include "build.hpp"
#include "utils.hpp"

static std::string
get_result_file(const std::string& query, QueryConfig& qconf)
{
	return Build::BinaryDir() + "/test_results/" + std::to_string(qconf.scale_factor) + "/" + query + ".result";
}

static bool
get_result_from_file(std::string& r, const std::string& query, QueryConfig& qconf)
{
	const auto file = get_result_file(query, qconf);
	std::cerr << "Reading result from file '" << file << "' ... ";
	if (!FileUtils::exists(file)) {
		std::cerr << "failed" << std::endl;
		return false;
	}

	r = FileUtils::read_string_from_file(file);
	std::cerr << "successful" << std::endl;
	return true;
}

#include "bench_tpch_rel.hpp"
#include "compiler.hpp"

typedef BenchmarkQuery (*QueryGenerator)(QueryConfig&);

static std::unordered_map<std::string, QueryGenerator> plans = {
	{ "q1", &tpch_rel_q1 },
	{ "q6", &tpch_rel_q6 },
	{ "q6a", &tpch_rel_q6 }, // alias
	{ "q6b", &tpch_rel_q6b },
	{ "q6c", &tpch_rel_q6c },
	{ "q3", &tpch_rel_q3 },
	{ "q9", &tpch_rel_q9 },
	{ "q9a", &tpch_rel_q9a },
	{ "q9b", &tpch_rel_q9b },
	{ "q9c", &tpch_rel_q9c },
	{ "q18", &tpch_rel_q18 },
	{ "s1", &tpch_rel_s1 },
	{ "s2", &tpch_rel_s2 },
	{ "j1", &tpch_rel_j1 },
	{ "j2", &tpch_rel_j2 },
	{ "j2a", &tpch_rel_j2a },
	{ "j2b", &tpch_rel_j2b },
	{ "j2c", &tpch_rel_j2c },
	{ "aggr1", &tpch_rel_aggr1 },
	{ "aggr1a", &tpch_rel_aggr1a },
	{ "imv1", &tpch_rel_imv1 }
};

BenchmarkQuery
prepare_tpch_query(QueryConfig& qconf, const std::string& q)
{
	auto it = plans.find(q);

	if (it == plans.end()) {
		std::cerr << "Invalid query '" << q << "'" << std::endl;
		ASSERT(false);
		exit(1);
	}

	qconf.query_name = q;

	BenchmarkQuery bq = (*it->second)(qconf);

	std::string str;
	if (get_result_from_file(str, q, qconf)) {
		bq.result = str;
		ASSERT(bq.result.size() > 0);
	}

	return bq;
}
