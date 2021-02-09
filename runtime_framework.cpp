#include "runtime_framework.hpp"
#include "runtime_utils.hpp"
#include "runtime_struct.hpp"
#include "runtime_vector.hpp"
#include "build.hpp"
#include <tbb/tbb.h>

std::string write_strFrom_Flavor(QueryConfig::Flavor b) {
	switch (b) {
	case QueryConfig::Flavor::Hyper:
		return std::string("hyper");
	case QueryConfig::Flavor::Vectorwise:
		return std::string("vector");
	default:
		ASSERT(false);
		return std::string("???");
	} 
}

std::string write_strFrom_bool(bool b) {
	return std::string(b ? "true" : "false");
}

std::string write_strFrom_str(const std::string& s) {
	return s;
}

std::string write_strFrom_int64_t(int64_t s) {
	return std::to_string(s);
}

std::string write_strFrom_int(int64_t s) {
	return write_strFrom_int64_t(s);
}

std::string write_strFrom_size_t(size_t s) {
	return std::to_string(s);
}


void
QueryConfig::write(std::ostream& o, const std::string& sep)
{
	const std::string prefix;

#define WRITE(tpe, val, _) { \
		o << prefix << #val << sep << write_strFrom_##tpe(val) << sep; \
	}

	EXPAND_CONFIG_ITEMS(WRITE, 0)

#undef WRITE
}

void
QueryResult::reset()
{
	if (init) {
		printf("got %d rows\n", rowid);
	} else {
		init = true;
	}
	colid = 0;
	rowid = 0;

	gathered.clear();
	gathered.str("");
}

void
QueryResult::begin_line()
{
	mutex.lock();
}

void
QueryResult::push_col(const std::string& s)
{
	if (colid) {
		gathered << "|";
//		printf("|");
	}
	gathered << s; 
	colid++;

//	printf("%s", s.c_str());
}

void
QueryResult::push_col(const char* buf, size_t len)
{
	push_col(std::string(buf, len));
}

void
QueryResult::end_line()
{
	// printf("\n");
	gathered << std::endl;
	colid=0;
	rowid++;
	// printf("\n");

	mutex.unlock();
}

std::string
QueryResult::finalize()
{
	return gathered.str();
}

Query::Query(QueryConfig& cfg)
 : config(cfg)
{
	primitives = new Primitives();
	config.check_result = false;
}

Query::~Query()
{
	for (auto& threads : pipelines) {
		for (auto& pipeline : threads) {
			delete pipeline;
		}
	}

	for (auto& local : locals) {
		delete local;
	}
	delete primitives;
}

void
Query::reset()
{
	result.reset();

	IResetableList::reset();

	for (auto& local : locals) {
		local->reset();
	}

	for (auto& ps : pipelines) {
		for (auto& p : ps) {
			p->reset();
		}
	}
}

#include <fstream>
#include <thread>

void
Query::run_pipeline(size_t p, size_t id, bool last)
{
	auto& pipe = pipelines[id][p];
	pipe->last = last;

	// LOG_DEBUG("Pipeline %d %s\n", p, pipe->last ? "last pipeline" : "");
	pipe->query_run();
};

void
Query::run(size_t run_no)
{
	ASSERT(pipelines.size() > 0);
	size_t num_p = pipelines[0].size();
	ASSERT(config.num_threads > 0);

	

	if (config.num_threads == 1) {
		for (size_t p=0; p<num_p; p++) {
			size_t id = 0;
			bool last = p+1 == num_p;

			run_pipeline(p, id, last);
		}
	} else {
		for (size_t p=0; p<num_p; p++) {
			bool last = p+1 == num_p;

#if 0
			if (last) {
				run_pipeline(p, 0, last);
				continue;
			}
#endif

			tbb::parallel_for<size_t>(0, config.num_threads, 1,
				[=](size_t id) {
					run_pipeline(p, id, last);
				});
		}
	}
}

IThreadLocal&
Query::_get_thread_local(size_t id)
{
	ASSERT(locals.size() > 0);
	ASSERT(id < locals.size());

	IThreadLocal* r = locals[id];
	ASSERT(r);
	return *r;
}

IThreadLocal&
Query::_get_thread_local(IPipeline& p)
{
	return _get_thread_local(p.thread_id);
}

#include "utils.hpp"

bool
Query::check_result()
{
	ASSERT(config.check_result);

	auto gathered = result.finalize();
	bool wrong = false;

	const char newline = '\n';
	auto gvec = split(gathered, newline);
	auto evec = split(result.expected, newline);

	ASSERT(gvec.size() == result.num_rows());

	if (gvec.size() != evec.size()) {
		wrong = true;
		std::cerr << "Number of rows do not match! Expected: " << evec.size() << ". Got: " << gvec.size() << std::endl;
	}

	static constexpr bool ingore_order = true;

	if (ingore_order) {
		auto sort = [] (auto& v) {
			std::sort(v.begin(), v.end());
		};

		sort(gvec);
		sort(evec);
	}

	size_t lines_wrong = 0;

	for (size_t row=0; row<evec.size(); row++) {
		auto& exp = evec[row];
		if (row < gvec.size()) {
			auto& got = gvec[row];

			if (exp != got) {
				std::cerr << "Mismatching row " << row << "." << std::endl
					<< "Expected: " << exp << std::endl
					<< "Got     : " << got << std::endl;
				lines_wrong++;
			}
		} else {
			std::cerr << "Missing row " << row << ": " << exp << std::endl;
			lines_wrong++;
		}

		if (lines_wrong > 100) {
			std::cerr << "Too many differences ... Aborting" << std::endl;
			break;
		}
	}

	if (wrong || lines_wrong > 0) {
		return false;
	} else {
		std::cerr << "Result are correct" << std::endl;
		return true;
	}
}

void
Query::print_result()
{
	std::cout << "Results:" << std::endl
		<< result.finalize() << std::endl
		<< "got " << result.num_rows() << " rows" << std::endl
		<< std::endl;
}

void
Query::write_result_to_file(const std::string& fname)
{
	FileUtils::write_string_to_file(fname, result.finalize());
}

IPipeline::IPipeline(Query& q, size_t thread_id)
 : query(q), thread_id(thread_id), last(false)
{
}

IPipeline::~IPipeline()
{
}

void
IPipeline::run()
{
}


IThreadLocal::IThreadLocal(Query& q)
 : query(q) {
}

IThreadLocal::~IThreadLocal()
{
}

void
IThreadLocal::reset()
{

}


void
IFujiObject::print_stats(std::ostream& o)
{
	for (auto& obj : objects) {
		obj.second->print_stats(o);
	}
}

IFujiObject::~IFujiObject()
{
}


FujiPipeline::FujiPipeline(Query& q, size_t thread_id, const char* _dbg_name)
 : IPipeline(q, thread_id)
{
	dbg_name = _dbg_name;
}

void
FujiPipeline::print_stats(std::ostream& o)
{
	// o << dbg_name << ": "<< std::endl;
}

void
FujiPipeline::post_run()
{
	// print_stats(std::cout);

	objects.clear();
}
