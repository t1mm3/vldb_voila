#ifndef H_RUNTIME_UTILS
#define H_RUNTIME_UTILS

#include <string>
#include <vector>
#include "runtime.hpp"

struct IResetable {
	virtual void reset() = 0;
};

struct IResetableList : IResetable {	
	virtual void reset() override;
	void add_resetable(IResetable* r);

private:
	std::vector<IResetable*> resetables;
};

void
runtime_partition(size_t* RESTRICT dest_counts, char** RESTRICT dest_data,
	size_t num_parts, char* RESTRICT data, size_t width, u64* hashes,
	size_t num, size_t offset);

#endif