#ifndef H_RUNTIME_FRAMEWORK
#define H_RUNTIME_FRAMEWORK

#include <string>
#include <vector>
#include <functional>
#include <sstream>
#include <limits>
#include <mutex>
#include "runtime_utils.hpp"
#include "runtime.hpp"

struct Query;
struct IPipeline;
struct IThreadLocal;
struct Primitives;
struct BlendSpacePoint;

namespace runtime {
	struct Database;
}

struct Framework;

typedef std::string str;


struct QueryConfig {
	enum Flavor {
		Hyper, Vectorwise, Fuji
	};
	runtime::Database& db;
	std::string mode;

#define EXPAND_CONFIG_ITEMS(F, ARG) \
	F(str,query_name,""); \
	F(Flavor,flavor,Flavor::Hyper); \
	\
	F(bool,safe_mode,true); \
	F(size_t,num_tuples,0); \
	F(size_t,vector_size,1024); \
	F(size_t,morsel_size,16*1024); \
	F(size_t,num_threads,1); \
	F(bool,optimized,true); \
	F(bool,run_query,true); \
	\
	F(size_t,num_hot_reps,3); \
	F(bool,print_result,true); \
	F(bool,check_result,false); \
	F(bool,fail_on_wrong_results,true); \
	F(size_t,scale_factor,0); \
	F(bool,allow_full_evaluation,true); \
	F(bool,hyper_compiler_vectorize_pipeline,false); \
	F(str,default_blend, ""); \
	F(str,blend_key_check, ""); \
	/* F(str,blend_payload_gather, ""); */ \
	F(str,blend_aggregates, ""); \


	bool adaptive_ht_chaining = true;
	size_t max_card = 1ull << 40ull;
	double hash_dmin = std::numeric_limits<hash_t>::min();
	double hash_dmax = std::numeric_limits<hash_t>::max();

	size_t min_table_extend = 1024*vector_size;
	size_t min_bucket_count = 1024*2;

	bool skip_translate = false;

	int timeout_seconds = 0;

	std::string cxx_compiler = "g++";
	std::string write_result_to_file;
	std::string compare_result_to_file;

	std::string write_profile_to_file;

	std::unordered_map<int, std::string> pipeline_default_blend;
	const BlendSpacePoint* full_blend = nullptr;

	//! Enables all possible blends. Even without actual args ... to count #BLENDs
	bool all_blends = false;


	#define DECL(tpe, name, val) tpe name = val;

EXPAND_CONFIG_ITEMS(DECL, 0)

	#undef DECL

	QueryConfig(runtime::Database& db) : db(db) {
	}

	void write(std::ostream& o, const std::string& sep = ",");	
};

struct IResetable;

struct QueryResult {
	void reset();

	std::string expected;
private:
	size_t colid = 0;
	size_t rowid = 0;

	std::ostringstream gathered;

	std::mutex mutex;

public:
	void begin_line();
	void push_col(const std::string& s);
	void push_col(const char* buf, size_t len);
	void end_line();

	size_t num_rows() const {
		return rowid;
	} 

	std::string finalize();

private:
	bool init = false;

};

struct Query : IResetableList {
	QueryConfig& config;
	QueryResult result;

public:
	Primitives* primitives;

private:
	std::vector<std::vector<IPipeline*>> pipelines;

	std::vector<IThreadLocal*> locals;

	IThreadLocal& _get_thread_local(size_t id);
	IThreadLocal& _get_thread_local(IPipeline& p);

	void run_pipeline(size_t p, size_t id, bool last);
public:
	void run(size_t run_no);

	bool check_result();
	void print_result();
	void write_result_to_file(const std::string& fname);

	template<typename T> T&
	get_thread_local(IPipeline& p) {
		return (T&)_get_thread_local(p);
	}


	template<typename S, typename T>void init(const S& f, const T& m) {
		for (size_t t=0; t<config.num_threads; t++) {
			locals.push_back(m(t, config.num_threads));
		}
		for (size_t t=0; t<config.num_threads; t++) {
			pipelines.push_back(f(t, config.num_threads));
		}
	}

	void reset() override;

	Query(QueryConfig& cfg);
	virtual ~Query();
};

struct IPipeline : IResetableList {
	Query& query;
	const size_t thread_id;

	IPipeline(Query& q, size_t thread_id);
	virtual ~IPipeline();

	bool last;

	virtual void run();

	virtual void post_run() {

	}
	virtual void pre_run() {

	}

	void query_run() {
		pre_run();

		run();

		post_run();
	}
};

struct IThreadLocal {
	Query& query;

	IThreadLocal(Query& q);
	virtual ~IThreadLocal();

	virtual void reset();
};

struct IFujiObject {
	const char* dbg_name = nullptr;
	std::vector<std::pair<int, IFujiObject*>> objects;

	virtual void print_stats(std::ostream& o);

	virtual ~IFujiObject();
};

struct FujiPipeline : IPipeline, IFujiObject {
	FujiPipeline(Query& q, size_t thread_id, const char* _dbg_name);

	void print_stats(std::ostream& o) override;

	void post_run() override;
};


#endif