#ifndef H_BENCHMARK_WAIT
#define H_BENCHMARK_WAIT

#include <string>
#include <vector>

struct BenchmarkWait {
	std::vector<std::string> blacklist = {"cored", "gcc", "make"};
	std::vector<std::string> whitelist = {"voila", "explorer", "gdb"};

	void operator()() const;
};

#endif