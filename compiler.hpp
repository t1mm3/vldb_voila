#ifndef H_COMPILER
#define H_COMPILER

#include <vector>
#include <iostream>
#include <fstream>
#include "build.hpp"
#include "bench.hpp"
#include <unordered_set>

struct Statement;
typedef std::shared_ptr<Statement> StmtPtr;

struct QueryConfig;

struct Compiler {
private:
	static void call_cxx_compiler(const std::string& ifname,
		const std::string& ofname, const QueryConfig& c);

public:
	const std::string share_fname;
	const std::string tmp_fname;
	const std::string exe_fname;
	const int thread_id;

	double t_cgen_ms = 0.0;
	double t_ccomp_ms = 0.0;	

	Compiler(int thread_id, const std::string& postfix = "")
	 : share_fname("voila_compiler_" + postfix + "")
	 , tmp_fname("/tmp/voila" + postfix + ".cpp")
	 , exe_fname("/tmp/run" + postfix + "")
	 , thread_id(thread_id)
	{}

private:
	void _compile(QueryConfig& config, BenchmarkQuery& p);
	void _run(QueryConfig& config, BenchmarkQuery& p,
		void* library);

public:
	std::string compile(QueryConfig& config, BenchmarkQuery& p);
	std::string run(QueryConfig& config, BenchmarkQuery& p);

	double _generate(QueryConfig& config, BenchmarkQuery& p,
		std::stringstream& s);

	struct ProgramInfo {
		struct Operator {
			const std::string name;
			std::unordered_set<StmtPtr> blends;

			Operator(const std::string& name)
			 : name(name) {
			}
		};
		struct Pipeline {
			std::vector<std::shared_ptr<Operator>> ops;
			bool interesting = true;

			size_t get_num_blend_points() const {
				size_t r = 0;
				for (auto& op : ops) {
					r += op->blends.size();
				}
				return r;
			}
		};
		std::vector<std::shared_ptr<Pipeline>> pipelines; 

		bool valid = false;
	};

	ProgramInfo get_info(QueryConfig& config, BenchmarkQuery& p);
};



#endif