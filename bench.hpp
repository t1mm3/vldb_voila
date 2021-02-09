#ifndef H_BENCH
#define H_BENCH

#include <string>
#include <memory>
#include <unordered_map>

namespace relalg {
struct RelOp;	
}

struct Program;


struct BenchmarkQuery {
	Program* prog = nullptr;
	std::shared_ptr<relalg::RelOp> root;

	std::string result = "";

	/* Annotates pipeline with a price in percent of total cost
	 * This is needed for explorer when exploring per-pipeline
	 * flavors */ 
	std::unordered_map<int, int> expensive_pipelines;
};


#endif