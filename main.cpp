#include "voila.hpp"

#include <iostream>
#include <algorithm>
#include <thread>
#include <tbb/tbb.h>
#include "common/runtime/Import.hpp"
#include "utils.hpp"
#include "compiler.hpp"
#include "runtime_framework.hpp"
#include "libs/cxxopts.hpp"
#include "bench_tpch.hpp"

using namespace std;

int main(int argc, char* argv[]) {
	cxxopts::Options options("VOILA generated program", "");

	options.add_options()
		("h,help", "Show help")
		("data", "Data directory", cxxopts::value<std::string>()->default_value(Build::BinaryDir()))
		("r,hot_runs", "Repetitions", cxxopts::value<int>()->default_value("5"))
		("vector_size", "Vector size", cxxopts::value<int>()->default_value("1024"))
		("num_threads", "#Threads, seperated by", cxxopts::value<std::string>()->default_value(std::to_string(std::thread::hardware_concurrency())))
		("morsel_size", "Morsel size", cxxopts::value<int>()->default_value(std::to_string(16*1024)))
		("flavor", "Codegen flavor, seperated by ','", cxxopts::value<std::string>()->default_value("fuji"))
		("debug", "Debug aka non-optimized build")
		("optimized", "Enforce optimized build")
		("safe", "Enable safe mode")
		("skip_translate", "Skip translation step, but compile and run")
		("timeout", "Timeout in seconds", cxxopts::value<int>()->default_value("30"))
		("no-run", "Only compile, do not run query")
		("no-result", "Do not print results")
		("no-check", "Do not check query results")
		("s,scale_factor", "TPC-H scale factor", cxxopts::value<int>()->default_value("1"))
		("q,queries", "Queries to run, separated by ','", cxxopts::value<std::string>()->default_value("j1"))
		("compiler", "C++ compiler to use", cxxopts::value<std::string>()->default_value("g++"))
		("result", "Write result to file", cxxopts::value<std::string>()->default_value(""))
		("profile", "Write profile to file", cxxopts::value<std::string>()->default_value(""))
		("compare", "Compare result to file", cxxopts::value<std::string>()->default_value(""))
		("default_blend", "Options for the default blend", cxxopts::value<std::string>()->default_value(""))
		("blend_key_check", "Options for hash key check", cxxopts::value<std::string>()->default_value(""))
		// ("blend_payload_gather", "Options for join payload gathers", cxxopts::value<std::string>()->default_value(""))
		("blend_aggregates", "Options for aggregates", cxxopts::value<std::string>()->default_value(""))
		("mode", "Tag for later retrival", cxxopts::value<std::string>()->default_value("direct"))
		;

	try {
		auto cmd = options.parse(argc, argv);

		if (cmd.count("h")) {
			std::cout << options.help({"", "Group"}) << std::endl;
			exit(0);
		}

		// setup TPC-H
		runtime::Database db;

		auto scale_factor = cmd["s"].as<int>();
		setup_tpch(db, scale_factor, cmd["data"].as<std::string>());

		QueryConfig qconf(db);

		qconf.vector_size = cmd["vector_size"].as<int>();
		qconf.morsel_size = cmd["morsel_size"].as<int>();

		qconf.num_hot_reps= cmd["hot_runs"].as<int>();
		qconf.scale_factor= scale_factor;
		qconf.cxx_compiler = cmd["compiler"].as<std::string>();
		qconf.write_result_to_file = cmd["result"].as<std::string>();
		qconf.compare_result_to_file = cmd["compare"].as<std::string>();
		qconf.write_profile_to_file = cmd["profile"].as<std::string>();

		qconf.default_blend = cmd["default_blend"].as<std::string>();
		qconf.blend_key_check = cmd["blend_key_check"].as<std::string>();
		// qconf.blend_payload_gather = cmd["blend_payload_gather"].as<std::string>();
		qconf.blend_aggregates = cmd["blend_aggregates"].as<std::string>();
		qconf.mode = cmd["mode"].as<std::string>();
		qconf.safe_mode = false;
		if (cmd.count("safe")) {
			qconf.safe_mode = true;
			qconf.timeout_seconds = cmd["timeout"].as<int>();
		}

		if (cmd.count("skip_translate")) {
			qconf.skip_translate = true;
		}

#ifdef IS_DEBUG
		qconf.optimized = false;
#endif
		if (cmd.count("debug")) {
			qconf.optimized = false;
		}
		if (cmd.count("optimized")) {
			qconf.optimized = true;
		}
		if (cmd.count("no-run")) {
			qconf.run_query = false;
		}
		if (cmd.count("no-result")) {
			qconf.print_result = false;
		}
		if (cmd.count("no-check")) {
			qconf.check_result = false;
		}

		if (qconf.compare_result_to_file.size() > 0) {
			qconf.check_result = true;			
		}

		const auto flavors = split(cmd["flavor"].as<std::string>(), ',');
		const auto queries = split(cmd["q"].as<std::string>(), ',');
		const auto num_threads_collection = split(cmd["num_threads"].as<std::string>(), ',');
		for (auto& threads : num_threads_collection) {
			qconf.num_threads = std::stoi(threads);

			tbb::task_scheduler_init scheduler(qconf.num_threads);

			// generate query
			Program program;

			for (auto& flavor : flavors) {
				if (!flavor.compare("hyper")) {
					qconf.flavor = QueryConfig::Flavor::Hyper;
				} else if (!flavor.compare("vector") || !flavor.compare("vectorwise")) {
					qconf.flavor = QueryConfig::Flavor::Vectorwise;
				} else if (!flavor.compare("fuji")) {
					qconf.flavor = QueryConfig::Flavor::Fuji;
				} else {
					std::cerr << "Invalid flavor '" << flavor << "'" << std::endl;
					exit(EXIT_FAILURE);
				}

				if (!qconf.default_blend.empty() && qconf.flavor != QueryConfig::Flavor::Fuji) {
					std::cerr << "Blend specification is only allowed with the 'fuji' backend" << std::endl;
					exit(EXIT_FAILURE);		
				}

				for (auto& q : queries) {
					Compiler compiler(0);
					auto bq = prepare_tpch_query(qconf, q);
					std::string res;

					res = compiler.compile(qconf, bq);
					if (res.empty()) {
						res = compiler.run(qconf, bq);
					}
				}
			}

			scheduler.terminate();
		}
	} catch (const cxxopts::OptionException& e) {
		std::cerr << "error parsing options: " << e.what() << std::endl;
		std::cout << options.help({"", "Group"}) << std::endl;
		exit(1);
	}

	return 0;
}