#include "voila.hpp"

#include <iostream>
#include <algorithm>
#include <thread>
#include <random>
#include <tbb/tbb.h>
#include "common/runtime/Import.hpp"
#include "utils.hpp"
#include "libs/cxxopts.hpp"
#include "bench_tpch.hpp"
#include "explorer_helper.hpp"
#include "build.hpp"
#include "compiler.hpp"
#include "blend_space_point.hpp"

using namespace std;

static int g_explore_seed = 0;

static int g_explore_threads = 1;

#define REL_ASSERT(x) if (!(x)) { std::cerr << "Assertion '" << #x << "' failed @ " << __LINE__ << std::endl; }

static size_t g_explore_count_generate = 0;

static int fd_lock = 0;

static std::mutex g_mutex;

static bool g_discover_blend_points = false;

bool
compile(const QueryConfig& qconf, const BenchmarkQuery& query,
	int thread_id_int, const std::string& thread_id)
{
	auto _compile = [&] () {
		{
			// FdLockGuard guard(fd_lock);
			std::lock_guard<std::mutex> guard(g_mutex);
			g_explore_count_tries++;			
		}

		QueryConfig conf(qconf);
		BenchmarkQuery q(query);
		// printf("RUN: compile\n");
		Compiler compiler(thread_id_int, thread_id);

		const std::string msg(compiler.compile(conf, q));
		if (!msg.empty()) {
			// dead end
			std::cerr << "Cannot generate '" << msg << "'" << std::endl;
			return false;
		}

		{
			std::lock_guard<std::mutex> guard(g_mutex);
			g_explore_count_generate++;	
		}
		
		if (g_explore_dry) {
			return true;
		}

		std::string res;
		{
			// FdLockGuard guard(fd_lock);
			std::lock_guard<std::mutex> guard(g_mutex);
			res = compiler.run(conf, q);
		}

		return res.empty();
	};

	bool success = _compile();

	if (success) {
		// FdLockGuard guard(fd_lock);
		std::lock_guard<std::mutex> guard(g_mutex);
		g_explore_count_success++;
	} else {
		std::cerr << "Failed" << std::endl;
	}

	return success;
}


#include <thread>
#include <vector>

struct ParallelExec {
	template<typename T>
	static void
	run(size_t num_threads, const T& fun) {
		std::vector<std::thread> m_threads;
		m_threads.reserve(num_threads);

		for (size_t i=0; i<num_threads; i++) {
			m_threads.push_back(std::thread(fun, i));
		}

		for (auto& t : m_threads) {
			t.join();
		}
		printf("Done\n");
	}
};


void
backtrack_per_pipeline(QueryConfig& qconf, BenchmarkQuery& query, size_t depth,
	int thread_id_int, const std::string& thread_id,
	const std::vector<int>& pipeline_ids, const std::vector<BlendConfig*>& blends)
{
	if (depth == pipeline_ids.size()) {
		compile(qconf, query, thread_id_int, thread_id);
	}

	if (depth >= pipeline_ids.size()) {
		return;
	}

	for (auto& b : blends) {
		int pipeline = pipeline_ids[depth];

		auto fstr = b->to_string();
		qconf.pipeline_default_blend[pipeline] = fstr;

		printf("RUN: pipeline %d = %s\n", pipeline, fstr.c_str());
		backtrack_per_pipeline(qconf, query, depth+1, thread_id_int, thread_id,
			pipeline_ids, blends);
	}
}

static size_t g_explore_invalid = 0;


#include "progress_meter.hpp"

struct Progress : ProgressMeter {
	void output(double progress, double sec_to_finish) override {
		if (g_explore_sample_num) {
			printf("SAMPLE: %d%% done ... %d secs to go\n",
				(int)(progress*100.0), (int)sec_to_finish);
		}
	}

	Progress(size_t tot) : ProgressMeter(tot) {}
};

struct FullExplorer;

struct ExplorerThread {
	QueryConfig qconf;
	BenchmarkQuery bench_query;
	FullExplorer& parent;

	const size_t thread_id;
	BlendSpacePoint space_point;

	void run();

	bool compile();
	bool run_compiled();

	std::unique_ptr<Compiler> compiler;

	ExplorerThread(QueryConfig& qconf, BenchmarkQuery& bench_query,
		size_t thread_id, FullExplorer& parent);

};

#include <unordered_set>
struct FullExplorer {
	Progress progress;

#ifdef IS_DEBUG
	std::unordered_set<BlendSpacePoint> m_seen;
#endif
	QueryConfig& qconf;
	BenchmarkQuery& bench_query;

	
	template<typename T>
	T random_item(const std::vector<T>& c) {
		auto index = m_rand_gen() % c.size();
		return c[index];
	}

	BlendConfig* random_flavor(BlendConfig* base)
	{
		auto flags = m_flags;
		if (!base) {
			flags |= kGenBlendOnlyBase;
		}
		auto& bs = *generate_blends(flags);

		if (!base) {
			return random_item(bs);
		} 

		std::vector<BlendConfig*> filtered;
		filtered.reserve(bs.size());

		for (auto& b : bs) {
			if (valid_base_to_other(base, b, flags)) {
				filtered.push_back(b);
			}
		}

		return random_item(std::move(filtered));	
	}

	void
	print_progress()
	{
		progress();
		// ASSERT(false && "todo");
	}

public:

	std::mt19937_64 m_rand_gen;

	GenBlendFlags m_flags;
	bool m_blend_per_pipeline;
	bool m_only_interesting;


	FullExplorer(QueryConfig& qconf, BenchmarkQuery& bench_query, int level)
	 : progress(g_explore_sample_num), qconf(qconf), bench_query(bench_query),
	 	m_rand_gen(g_explore_seed) {

	 	switch (level) {
	 	case 0:
	 	case 1:
	 		m_flags = kGenBlendBinaryPrefetch | kGenBlendOnlyEssentialComp | kGenBlendOnlyEssentialFsm;
	 		m_blend_per_pipeline = level == 1;
	 		m_only_interesting = true;
	 		break;

	 	case 2:
	 	case 3:
	 		m_flags = 0;
	 		m_blend_per_pipeline = level == 3;
	 		m_only_interesting = true;
	 		break;

	 	case 4:
	 		m_flags = 0;
	 		m_blend_per_pipeline = true;
	 		m_only_interesting = false;
	 		break;

	 	default:
	 		std::cerr << "Invalid exploration level " << level << std::endl;
	 		exit(1);
	 		break;
	 	}
	}
	void operator()() {
		size_t thread_id = 1;

		if (g_explore_sample_num) {
			sample();
		} else {
			// auto space = new_space(thread_id);
			// backtrack(space);
		}
	}

	BlendSpacePoint get_space_point() {
		BlendSpacePoint space_point;
		qconf.all_blends = true;
		Compiler compiler(0, "info");

		auto info = compiler.get_info(qconf, bench_query);
		if (!info.valid) {
			std::cerr << "Cannot generate query" << std::endl;
			exit(1);
		}

		// create template
		for (auto& pipeline : info.pipelines) {
			BlendSpacePoint::Pipeline r_pipe; 

			r_pipe.ignore = !pipeline->interesting;

			r_pipe.point_flavors.resize(pipeline->get_num_blend_points());

			space_point.pipelines.emplace_back(r_pipe);
		}

		int i;

		i = 0;
		bool has_price = false;
		if (m_only_interesting) {
			for (auto& pipeline : space_point.pipelines) {
				auto price = bench_query.expensive_pipelines.find(i);
				if (price != bench_query.expensive_pipelines.end()) {
					has_price = true;
				}
				i++;
			}
		}

		printf("FULL: found %d pipelines\n", (int)space_point.pipelines.size());
		printf("FULL: m_only_interesting %d\n", m_only_interesting);

		if (!has_price) {
			std::cerr << "FULL: no pipeline prices attaced!!!!!" << std::endl;
			exit(1);
			ASSERT(has_price);
		}

		i = 0;
		for (auto& pipeline : space_point.pipelines) {
			printf("FULL: pipeline %d has %d blend points.",
				i, (int)pipeline.point_flavors.size());

			if (m_only_interesting && pipeline.ignore) {
				printf(" IGNORE");
			} else {
				printf(" ADD");
			}

			auto price = bench_query.expensive_pipelines.find(i);
			if (price == bench_query.expensive_pipelines.end()) {
				printf(" NO_PRICE");
				if (has_price && m_only_interesting) {
					pipeline.ignore = true;
				}
			} else {
				printf(" %d %%", price->second);
			}

			printf("\n");
			i++;
		}

		if (g_discover_blend_points) {
			exit(0);
		}

		return space_point;
	}



	void sample() {
		std::vector<ExplorerThread> expl_threads;
		expl_threads.reserve(g_explore_threads + 1);
		size_t expl_num = 0;

		const auto original_point = get_space_point();

		for (int i=0; i<g_explore_threads; i++) {
			expl_threads.emplace_back(ExplorerThread(qconf,
				bench_query, i, *this));
		}

		auto flush = [&] () {
			printf("flush\n");
			sleep(2);
			ParallelExec::run(g_explore_threads, [&] (auto i) {
				if (expl_num <= i) {
					return;
				}

				auto& t = expl_threads[i];
				t.compile();
			});

			for (auto& t : expl_threads) {
				t.run_compiled();
			}

			expl_num = 0;
		};

		
		while (1) {
			// FdLockGuard guard(fd_lock);
			// sampling mode: enough samples found
			if (g_explore_sample_num && g_explore_count_tries >= g_explore_sample_num) {
				flush();
				return;
			}

			auto gen = [&] (BlendSpacePoint& space_point) {
				// generate random point
				BlendConfig* base = random_flavor(nullptr);

				for (auto& pipeline : space_point.pipelines) {
					pipeline.flavor = base;
					if (m_blend_per_pipeline) {
						base = random_flavor(nullptr);
					}
				}

				for (auto& pipeline : space_point.pipelines) {
					// generate BLEND points
					for (auto& point : pipeline.point_flavors) {
						if (m_only_interesting && pipeline.ignore) {
							continue;
						}
						point = random_flavor(pipeline.flavor);
					}
				}
			};

			gen(expl_threads[expl_num].space_point);

			if (expl_threads[expl_num].space_point.is_valid()) {
				expl_num++;

				if (expl_num >= g_explore_threads) {
					flush();
				}
			} else {
				g_explore_invalid++;
			}
		}
	}
};

bool
ExplorerThread::compile()
{
	compiler = nullptr;

	{
		// FdLockGuard guard(fd_lock);
		std::lock_guard<std::mutex> guard(g_mutex);
		g_explore_count_tries++;			
	}

	qconf.pipeline_default_blend = {};
	qconf.full_blend = &space_point;

	std::cerr << "Compile Flavor: " << space_point.to_string() << std::endl;


	QueryConfig conf(qconf);
	BenchmarkQuery q(bench_query);
	// printf("RUN: compile\n");
	compiler = std::make_unique<Compiler>(thread_id, std::to_string(thread_id));

	const std::string msg(compiler->compile(conf, q));
	if (!msg.empty()) {
		// dead end
		std::cerr << "Cannot generate '" << msg << "'" << std::endl;

		compiler = nullptr;
		return false;
	}

	g_explore_count_generate++;	
	qconf.full_blend = nullptr;

	return true;
}

bool
ExplorerThread::run_compiled()
{
	qconf.pipeline_default_blend = {};
	qconf.full_blend = &space_point;

	std::cerr << "Run Flavor: " << space_point.to_string() << std::endl;


	QueryConfig conf(qconf);
	BenchmarkQuery q(bench_query);

	if (!compiler) {
		return false;
	}

	std::string res;
	if (g_explore_dry) {
		res = "";
	} else {
		// FdLockGuard guard(fd_lock);
		std::lock_guard<std::mutex> guard(g_mutex);
		res = compiler->run(conf, q);
	}

	if (res.empty()) {
		g_explore_count_success++;
	}
	qconf.full_blend = nullptr;
	return res.empty();
}

void
ExplorerThread::run()
{
	qconf.pipeline_default_blend = {};
	qconf.full_blend = &space_point;

	// ASSERT(false);

	std::cerr << "Run Flavor: " << space_point.to_string() << std::endl;
	::compile(qconf, bench_query, thread_id, std::to_string(thread_id));

	qconf.full_blend = nullptr;
}

ExplorerThread::ExplorerThread(QueryConfig& qconf, BenchmarkQuery& bench_query,
	size_t thread_id, FullExplorer& parent)
 : qconf(qconf), bench_query(bench_query), parent(parent), thread_id(thread_id), space_point(parent.get_space_point()) {
	
}

static int
random_seed() {
	std::random_device r;
	return r();
}

int main(int argc, char* argv[]) {
	cxxopts::Options options("VOILA generated program", "");

	options.add_options()
		("h,help", "Show help")
		("data", "Data directory", cxxopts::value<std::string>()->default_value(Build::BinaryDir()))
		("r,hot_runs", "Repetitions", cxxopts::value<int>()->default_value("3"))
		("vector_size", "Vector size", cxxopts::value<int>()->default_value("1024"))
		("num_threads", "#Threads", cxxopts::value<int>()->default_value(std::to_string(std::thread::hardware_concurrency())))
		("morsel_size", "Morsel size", cxxopts::value<int>()->default_value(std::to_string(16*1024)))
		("debug", "Debug aka non-optimized build")
		("optimized", "Enforce optimized build")
		("s,scale_factor", "TPC-H scale factor", cxxopts::value<int>()->default_value("1"))
		("seed", "Random seed used for sampling", cxxopts::value<int>()->default_value(std::to_string(random_seed())))
		("q,query", "Query to run", cxxopts::value<std::string>()->default_value("q9"))
		("compiler", "C++ compiler to use", cxxopts::value<std::string>()->default_value("g++"))
		("unsafe", "Do not use safe mode")
		("no-check", "Do not check query results")
		("base", "Explore only base flavors")
		("pipeline", "Explore base flavors for expensive pipelines")
		("full", "Full exploration. With level. 0: limited, no-pipeline flavors; 1: limited, per-pipeline "
			"2: unlimited, no-pipeline, 3: unlimited, per-pipeline, 4: like 3 but also including uninteresting pipeline",
			cxxopts::value<int>()->default_value("1"))
		("list-base", "List base flavors")
		("discover-points", "Discover blend points")
		("dry", "Dry run")
		("timeout", "Timeout in seconds", cxxopts::value<int>()->default_value("360"))
		("mode", "Tag for later retrival", cxxopts::value<std::string>()->default_value("explore"))
		("sample", "Sampling to <= n samples. <= 0 i.e. no sampling. Does not work (yet) with --pipeline or --base",
				cxxopts::value<int64_t>()->default_value("0"))
		("explore_threads", "#Threads for exploration/compilation (actual runs with run sequentially using --num_threads cores/threads)",
			cxxopts::value<int>()->default_value(std::to_string(std::thread::hardware_concurrency())))
		("lock_file", "Lock file to use", cxxopts::value<std::string>()->default_value("/tmp/voila_explorer.lock"))
		;



	try {
		auto cmd = options.parse(argc, argv);

		if (cmd.count("h")) {
			std::cout << options.help({"", "Group"}) << std::endl;
			exit(0);
		}

		if (cmd.count("list-base")) {
			const auto& flavors = *generate_blends(kGenBlendOnlyBase);
			for (const auto& flavor : flavors) {
				std::cout << flavor->to_string() << std::endl;
			}
			return 0;
		}

		if (cmd.count("discover-points")) {
			g_discover_blend_points = true;
		}

		// setup TPC-H
		runtime::Database db;

		auto scale_factor = cmd["s"].as<int>();
		setup_tpch(db, scale_factor, cmd["data"].as<std::string>());

		QueryConfig qconf(db);
		
		if (cmd.count("no-check")) {
			qconf.check_result = false;
			qconf.fail_on_wrong_results = false;
		}

		qconf.vector_size = cmd["vector_size"].as<int>();
		qconf.morsel_size = cmd["morsel_size"].as<int>();

		qconf.num_hot_reps= cmd["hot_runs"].as<int>();
		qconf.scale_factor= scale_factor;
		qconf.cxx_compiler = cmd["compiler"].as<std::string>();

		fd_lock = open(cmd["lock_file"].as<std::string>().c_str(), O_CREAT, S_IRWXU|S_IRWXG);
		FdLockGuard lock_guard(fd_lock);

		if (cmd.count("base") > 0) {
			if (g_explore_mode != ExploreMode::Unknown) {
				std::cerr << "Can only set one mode" << std::endl;
				exit(1);
			}
			g_explore_mode = ExploreMode::OnlyBase;
		}

		if (cmd.count("pipeline") > 0) {
			if (g_explore_mode != ExploreMode::Unknown) {
				std::cerr << "Can only set one mode" << std::endl;
				exit(1);
			}
			g_explore_mode = ExploreMode::PerPipelineBase;
		}

		if (cmd.count("full") > 0) {
			if (g_explore_mode != ExploreMode::Unknown) {
				std::cerr << "Can only set one mode" << std::endl;
				exit(1);
			}
			g_explore_mode = ExploreMode::ExploreAll;
		}

		g_explore_sample_num = cmd["sample"].as<int64_t>() <= 0 ? 0 : cmd["sample"].as<int64_t>();

		switch (g_explore_mode) {
		case ExploreMode::OnlyBase:
			printf("MODE: explore base flavors\n");
			break;
		case ExploreMode::PerPipelineBase:
			printf("MODE: explore (expensive) per-pipeline base flavors\n");
			break;
		case ExploreMode::ExploreAll:
			printf("MODE: full explore\n");
			break;
		case ExploreMode::Unknown:
			std::cerr << "Unknown mode" << std::endl;
			exit(1);
			break;
		}

		if (g_explore_mode != ExploreMode::ExploreAll && g_explore_sample_num) {
			std::cerr << "Sampling is only supported for full exploration" << std::endl;
			exit(1);
		}


		qconf.mode = cmd["mode"].as<std::string>();;
		g_explore_threads = cmd["explore_threads"].as<int>();
		if (g_explore_threads < 1) {
			std::cerr << "--explore_threads=" << g_explore_threads << " is an invalid #threads" << std::endl;
			exit(1);
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

		g_explore_dry = cmd.count("dry") > 0;
		g_explore_seed = cmd["seed"].as<int>();

		qconf.check_result = true;			
		qconf.flavor = QueryConfig::Flavor::Fuji;
		qconf.safe_mode = !cmd.count("unsafe");
		qconf.timeout_seconds = cmd["timeout"].as<int>();

		qconf.num_threads = cmd["num_threads"].as<int>();
		ASSERT(qconf.num_threads > 0);

		tbb::task_scheduler_init scheduler(qconf.num_threads);

		const auto q = cmd["q"].as<std::string>();
		auto bench_query = prepare_tpch_query(qconf, q);

		switch (g_explore_mode) {
		case ExploreMode::OnlyBase:
			{
				const auto& default_blends = *generate_blends(kGenBlendOnlyBase);

				for (const auto& dblend : default_blends) {
					qconf.default_blend = dblend->to_string();
					printf("RUN: default_blend = %s\n", qconf.default_blend.c_str());

					compile(qconf, bench_query, 1, "1");
				}
			}
			break;

		case ExploreMode::PerPipelineBase:
			{
				const auto& blends = *generate_blends(kGenBlendOnlyBase | kGenBlendBinaryPrefetch);
				auto default_blend = blends[0];
				qconf.default_blend = default_blend->to_string();
				printf("RUN: default_blend = %s\n", qconf.default_blend.c_str());

				auto pipeline_ids = get_most_expensive_pipeline_ids(bench_query, 2);

				for (auto& p : pipeline_ids) {
					printf("RUN: modify pipeline %d\n", p);
				}

				backtrack_per_pipeline(qconf, bench_query, 0, 1, "1", pipeline_ids, blends);
			}
			break;

		case ExploreMode::ExploreAll:
			{
				FullExplorer explore(qconf, bench_query, cmd["full"].as<int>());

				explore();
			}
			break;

		default:
			std::cerr << "Unsupported mode" << std::endl;
			exit(1);
			break;
		}

		fprintf(stderr, "\n=== Summary ==\n");
		if (g_explore_sample_num) {
			fprintf(stderr, "Sampling:         %ld\n", g_explore_sample_num);
		} else {
			fprintf(stderr, "Sampling:         none\n");
		}
		fprintf(stderr, "Space Tested:     %d\n", (int)g_explore_count_tries);
		fprintf(stderr, "Space Ran:        %d\n", (int)g_explore_count_success);
		fprintf(stderr, "Space Compiled:   %d\n", (int)g_explore_count_generate);
		fprintf(stderr, "Space Invalid:    %d\n", (int)g_explore_invalid);
	
		scheduler.terminate();
	} catch (const cxxopts::OptionException& e) {
		std::cerr << "error parsing options: " << e.what() << std::endl;
		std::cout << options.help({"", "Group"}) << std::endl;
		exit(1);
	}

	return 0;
}
