#include "compiler.hpp"
#include "build.hpp"
#include <iostream>
#include <fstream>
#include "runtime.hpp"
#include "cg_vector.hpp"
#include "cg_hyper.hpp"
#include "cg_fuji.hpp"
#include "runtime_framework.hpp"
#include "utils.hpp"
#include "voila.hpp"
#include <sqlite3.h>
#include "safe_env.hpp"
#include "benchmark_wait.hpp"

struct CompilationError {
};

void Compiler::call_cxx_compiler(const std::string& ifname,
		const std::string& ofname, const QueryConfig& config) {
	std::stringstream rm;
	rm << "rm -f " << ofname;

	system(rm.str().c_str());

	std::stringstream s;
	s << config.cxx_compiler << " ";
#if defined(__PPC64__) || defined(__ppc64__) || defined(_ARCH_PPC64)
	if (config.optimized) {
                s << " -mcpu=native -mtune=native -O3 ";
        } else {
                s << " -mcpu=native -mtune=native -O0 -fsanitize=address ";
        }
#else
	if (config.optimized) {
		s << " -march=native -O3 ";
	} else {
		s << " -march=native -O0 -fsanitize=address ";
	}
#endif
#ifdef IS_DEBUG
	s << " -DIS_DEBUG ";
#endif

#ifdef IS_RELEASE
	s << " -DIS_RELEASE ";
#endif

	s	<< " -g -shared -fPIC --std=c++17 " // -L./ -Ldb-engine-paradigms/ -ldl -lpthread -lcommon -lvoila_runtime
		<< " -I " << Build::SourceDir() << "/db-engine-paradigms/include"
		<< " -I " << Build::SourceDir() 
		<< " -I ./"
		<< " -Wall " // -pedantic 
		<< ifname 
		<< " -o "
		<< ofname;
	int r = system(s.str().c_str());
	if (r) {
		ASSERT(!r);
		throw CompilationError();
	}
}

#include "runtime.hpp"
#include <chrono>
#include <dlfcn.h>

static std::string gen_int_str_mapping(const std::unordered_map<int, std::string>& m,
	const std::string& prefix)
{
	std::ostringstream o;
	bool first = true;

	for (auto& k : m) {
		if (!first) {
			o << ";";
		}
		o << prefix << k.first << ":" << k.second;

		first = false;
	}

	return o.str();
}


typedef Query* (*voila_query_new_t)(QueryConfig*);
typedef void (*voila_query_run_t)(Query*);
typedef void (*voila_query_reset_t)(Query*);
typedef void (*voila_query_free_t)(Query*);

#include "relalg_translator.hpp"
#include "printing_pass.hpp"
#include "PerfEvent.hpp"
#include <sqlite3.h>
struct sqlite3;

struct SqliteDB {
	sqlite3* db;

	SqliteDB(const std::string& name = "voila.db") {
		db = nullptr;
		if (sqlite3_open(name.c_str(), &db) != SQLITE_OK) {
			std::cerr << "Couldn't open database file: " << sqlite3_errmsg(db) << std::endl;
			ASSERT(false);
		}
	}

	void create_tables()
	{
		// try to create
		std::string sql("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT,"
			"t TIMESTAMP, mode TEXT, query TEXT, result TEXT, rep INTEGER, time_ms REAL NULL, "
			"tuples REAL NULL, cycles REAL NULL, instructions REAL NULL, L1misses REAL NULL, "
			"LLCmisses REAL NULL, branchmisses REAL NULL, taskclock REAL NULL, threads INTEGER, "
			"cgen_ms REAL NULL, ccomp_ms REAL NULL, default_blend TEXT, key_check_blend TEXT, "
			"aggregates_blend TEXT, backend TEXT, scale_factor INTEGER, pipeline_flavor TEXT, "
			"full_blend TEXT);");
		char* err_msg;
		int rc = sqlite3_exec(db, sql.c_str(), 0, 0, &err_msg);
		sqlite3_free(err_msg);
	}

	~SqliteDB() {
		if (db) {
			sqlite3_close(db);
		}
	}
};

static void record_run(SqliteDB& sqlite_db, const QueryConfig& config, PerfEvent* events,
	const std::string& result, double t_ms, int rep, bool invalid, double cgen_ms,
	double ccomp_ms)
{
	sqlite3_stmt *stmt;
	sqlite3_prepare_v2(sqlite_db.db, "INSERT INTO runs(t, mode, query, result, " // 1 - 3
			"rep, time_ms, tuples, cycles, instructions, L1misses, LLCmisses, branchmisses, taskclock, " // 4 - 11
			"threads, cgen_ms, ccomp_ms, default_blend, key_check_blend, aggregates_blend, backend, " // 12 -
			"scale_factor, pipeline_flavor, full_blend)"
		"VALUES (datetime('now','localtime'), ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, "
			"?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22);", -1, &stmt, NULL);

	int index = 0;

	index++;
	sqlite3_bind_text(stmt, index, config.mode.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	sqlite3_bind_text(stmt, index, config.query_name.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	sqlite3_bind_text(stmt, index, result.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	sqlite3_bind_int(stmt, index, rep);

	index++;
	if (invalid) {
		sqlite3_bind_double(stmt, index, t_ms); 
	} else {
		sqlite3_bind_null(stmt, index);
	}

	index++;
	if (config.num_tuples > 0) {
		sqlite3_bind_int(stmt, index, config.num_tuples);
	} else {
		sqlite3_bind_null(stmt, index);
	}

	std::cerr << "Run finished with '" << result << "'"<< std::endl; 

	std::vector<std::string> counters = {"cycles", "instructions", "L1-misses", "LLC-misses", "branch-misses", "task-clock"};
	for (const auto& name : counters) {
		index++;
		if (events && events->hasCounter(name)) {
			sqlite3_bind_double(stmt, index, events->getCounter(name)); 
		} else {
			sqlite3_bind_null(stmt, index); 
		}
			
	}

	index++;
	sqlite3_bind_int(stmt, index, config.num_threads);

	index++;
	if (invalid) {
		sqlite3_bind_double(stmt, index, cgen_ms); 
	} else {
		sqlite3_bind_null(stmt, index);
	}

	index++;
	if (invalid) {
		sqlite3_bind_double(stmt, index, ccomp_ms); 
	} else {
		sqlite3_bind_null(stmt, index);
	}

	index++;
	sqlite3_bind_text(stmt, index, config.default_blend.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	sqlite3_bind_text(stmt, index, config.blend_key_check.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	sqlite3_bind_text(stmt, index, config.blend_aggregates.c_str(), -1, SQLITE_TRANSIENT);

	index++;
	std::string backend;
	switch (config.flavor) {
	case QueryConfig::Flavor::Hyper:
		backend = "hyper";
		break;

	case QueryConfig::Flavor::Vectorwise:
		backend = "vectorwise";
		break;

	case QueryConfig::Flavor::Fuji:
		backend = "fuji";
		break;

	default:
		backend = "unknown";
		break;
	}
	sqlite3_bind_text(stmt, index, backend.c_str(), -1, SQLITE_TRANSIENT);


	index++;
	sqlite3_bind_int(stmt, index, config.scale_factor);


	index++;
	{
		if (config.pipeline_default_blend.size() > 0) {
			sqlite3_bind_text(stmt, index, gen_int_str_mapping(config.pipeline_default_blend, "pipeline").c_str(), -1, SQLITE_TRANSIENT);
		} else {
			sqlite3_bind_null(stmt, index);
		}
	}

	index++;
	{
		if (config.full_blend) {
			sqlite3_bind_text(stmt, index, config.full_blend->to_string().c_str(), -1, SQLITE_TRANSIENT);
		} else {
			sqlite3_bind_null(stmt, index);
		}
	}

	ASSERT(index == 22);
	int rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		std::cerr << "ERROR inserting data: " << sqlite3_errmsg(sqlite_db.db) << std::endl;
		ASSERT(false);
	}

	sqlite3_finalize(stmt);
}

std::string
Compiler::compile(QueryConfig& config, BenchmarkQuery& bq)
{
	SafeEnv safe(share_fname, thread_id, config.safe_mode,
		config.timeout_seconds);

	SafeEnv::Result status = safe([&] () {
		_compile(config, bq);
	});


	bool failure = status != SafeEnv::Result::Success;

	if (failure) {
		SqliteDB db;
		db.create_tables();

		std::string result = SafeEnv::result2str(status);
		std::cerr << "Failed '" << result << "'" << std::endl;
		std::transform(result.begin(), result.end(), result.begin(),
			[] (char c){ return std::toupper(c); });

		record_run(db, config, nullptr, result, 0.0, -1, true /* invalid */, 0, 0);
	}

	if (status == SafeEnv::Result::Success) {
		return "";
	} else {
		return SafeEnv::result2str(status);
	}
}


std::string
Compiler::run(QueryConfig& config, BenchmarkQuery& bq)
{
	SafeEnv safe(share_fname, thread_id, config.safe_mode,
		config.timeout_seconds);

	bool failure;
	std::string result;

	SafeEnv::Result status = SafeEnv::Result::Success;

	status = safe([&] () {
		std::cerr << "Loading " << exe_fname << std::endl;
		void* library = dlopen(exe_fname.c_str(), RTLD_NOW);

		_run(config, bq, library);

		std::cerr << "Closing " << exe_fname << std::endl;
		dlclose(library);
	});

	failure = status != SafeEnv::Result::Success;
	result = SafeEnv::result2str(status);

	if (failure) {
		SqliteDB db;
		db.create_tables();

		std::cerr << "Failed '" << result << "'" << std::endl;
		std::transform(result.begin(), result.end(), result.begin(),
			[] (char c){ return std::toupper(c); });

		record_run(db, config, nullptr, result, 0.0, -1, true /* invalid */, 0, 0);
	}

	if (status == SafeEnv::Result::Success) {
		return "";
	} else {
		return SafeEnv::result2str(status);
	}
}

template<typename T>
double measure_ms(const T& f)
{
	auto start_time = std::chrono::high_resolution_clock::now();
	f();
	auto end_time = std::chrono::high_resolution_clock::now();
	auto time = end_time - start_time;

	return time/std::chrono::milliseconds(1);
}

double Compiler::_generate(QueryConfig& config, BenchmarkQuery& bq,
		std::stringstream& s) {
	ASSERT(bq.prog || bq.root);

	return measure_ms([&] () {
		if (config.skip_translate) {
			return;
		}
		RelOpTranslator transl(config);
		transl(*bq.root);
		bq.prog = &transl.prog;

		Program& p = *bq.prog;
		switch (config.flavor) {
		case QueryConfig::Flavor::Hyper:
			{
				HyperCodegen hg(config);
				s << hg(p);	
			}
			break;
		case QueryConfig::Flavor::Vectorwise:
			{
				VectorCodegen vg(config);
				s << vg(p);
			}
			break;

		case QueryConfig::Flavor::Fuji:
			{
				FujiCodegen fg(config);
				s << fg(p);
			}
			break;
		default:
			ASSERT(false && "Unhandled flavor");
			break;
		}

		s << std::endl;
		// write source code
		FileUtils::write_string_to_file(tmp_fname, s.str());
	});
}

struct GetInfoPass : Pass {
	Compiler::ProgramInfo info;

	GetInfoPass() {
		flat = true;
	}

	size_t m_pipeline = 0;
	std::string m_lolepop;

	virtual void on_pipeline(Pipeline& p) {
		auto pipeline = std::make_shared<Compiler::ProgramInfo::Pipeline>();
		pipeline->interesting = p.tag_interesting;
		info.pipelines.emplace_back(pipeline);

		recurse_pipeline(p);

		m_pipeline++;
	}

	virtual void on_lolepop(Pipeline& p, Lolepop& l) {
		m_lolepop = l.name;
		info.pipelines.back()->ops.emplace_back(std::make_shared<Compiler::ProgramInfo::Operator>(l.name));
		recurse_lolepop(p, l);
	}

	virtual void on_statement(LolepopCtx& ctx, StmtPtr& s) {
		if (s->type == Statement::Type::BlendStmt) {
			auto& pipeline = info.pipelines.back();
			auto& op = pipeline->ops.back();

			printf("InfoPass: BLEND @ Pipeline %d, Op %s\n",
				(int)m_pipeline, m_lolepop.c_str());

			op->blends.insert(s);
		}

		recurse_statement(ctx, s, true);
		recurse_statement(ctx, s, false);
	}
};

Compiler::ProgramInfo
Compiler::get_info(QueryConfig& config, BenchmarkQuery& bq)
{
	RelOpTranslator transl(config);
	transl(*bq.root);
	bq.prog = &transl.prog;

	Program& p = *bq.prog;

	GetInfoPass info;
	info(p);

	info.info.valid = true;
	return info.info;
}

static BenchmarkWait bwait;

void
Compiler::_compile(QueryConfig& config, BenchmarkQuery& bq)
{
	ASSERT(bq.prog || bq.root);

	SqliteDB sqlite_db;
	std::stringstream s;

	std::cerr << "Generating" << std::endl;
	t_cgen_ms = _generate(config, bq, s);

	std::cerr << "Compiling" << std::endl;
	t_ccomp_ms = measure_ms([&] () {
		call_cxx_compiler(tmp_fname, exe_fname, config);
	});
}

void
Compiler::_run(QueryConfig& config, BenchmarkQuery& bq, void* library)
{
	std::cerr << "Waiting until machine is clear" << std::endl;

	bwait();

	SqliteDB sqlite_db;

	std::cerr << "Loading functions" << std::endl;

	voila_query_new_t qnew;
	voila_query_run_t qrun;
	voila_query_reset_t qreset;
	voila_query_free_t qfree;
	char* error;

#define SYM(dest, name) \
	*(void **) (&dest) = dlsym(library, name); \
	if ((error = dlerror()) != NULL)  { \
        std::cerr << error << std::endl; \
        exit(EXIT_FAILURE); \
    }

    SYM(qnew, 	"voila_query_new");
    SYM(qrun, 	"voila_query_run");
    SYM(qreset, "voila_query_reset");
    SYM(qfree, 	"voila_query_free");
#undef SYM

    Query* query;
    std::cerr << "Creating query" << std::endl;
    query = qnew(&config);
    ASSERT(query);

	if (config.compare_result_to_file.size() > 0 && config.check_result) {
		query->result.expected = FileUtils::read_string_from_file(config.compare_result_to_file);
	} else {
    	query->result.expected = bq.result;
	}

	std::cerr << "Running query" << std::endl;

	// set the configuration options
	g_config_full_evaluation = config.allow_full_evaluation;
	g_config_vector_size = config.vector_size;

	const auto& profile_path = config.write_profile_to_file;
	const bool profile = profile_path.size() > 0;

	double p_sum_time = 0.0;
	double p_min_time = 0.0;

	PerfEvent pevts;

	sqlite_db.create_tables();

    for (size_t r=0; r<config.num_hot_reps; r++) {
		qreset(query);

		std::string result = "UNKNOWN";
		double t_ms;

		try {
			pevts.startCounters();
			auto start_time = std::chrono::high_resolution_clock::now();
			qrun(query);
			auto end_time = std::chrono::high_resolution_clock::now();
			pevts.stopCounters();
			auto time = end_time - start_time;

			std::cerr << "Ran in " << time/std::chrono::milliseconds(1) << " ms" << std::endl;

			t_ms = time/std::chrono::milliseconds(1);
			p_sum_time += t_ms;
			if (!r) {
				p_min_time = t_ms;
			} else {
				p_min_time = std::min(p_min_time, t_ms);
			}

			if (config.write_result_to_file.size() > 0) {
				query->write_result_to_file(config.write_result_to_file);
			}
			if (config.print_result) {
				query->print_result();
			}

			if (config.check_result) {
				bool correct = query->check_result();
				if (!correct && config.fail_on_wrong_results) {
					ASSERT(false && "wrong results");
				}

				result = correct ? "OKAY" : "WRONG_RESULTS";
			} else {
				result = "UNCHECKED";
			}
			record_run(sqlite_db, config, &pevts, result, t_ms, r,
				true, t_cgen_ms, t_ccomp_ms);
		} catch (const std::bad_alloc& ex) {
			result = "OOM '" + std::string(ex.what()) + "'";		
			record_run(sqlite_db, config, &pevts, result, t_ms, r,
				true, t_cgen_ms, t_ccomp_ms);
		} catch (const std::exception& ex) {
			result = "EXCEPT '" + std::string(ex.what()) + "'";		
			record_run(sqlite_db, config, &pevts, result, t_ms, r,
				true, t_cgen_ms, t_ccomp_ms);
		}

    }

	if (profile) {
		if (!FileUtils::exists(profile_path)) {
			FileUtils::write_string_to_file(profile_path, "#header\n");
		}
		std::ofstream f;
		f.open(profile_path, std::ios_base::app);

		config.write(f);
		f	<< "avg," << p_sum_time / (double)config.num_hot_reps << ","
			<< "min," << p_min_time
			<< std::endl;
		f.close();
	}

	std::cerr << "Freeing query" << std::endl;
	qfree(query);
}
