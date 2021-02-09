#ifndef H_RUNTIME_HYPER
#define H_RUNTIME_HYPER

#include "runtime_framework.hpp"

struct HyperPipeline : IPipeline {
	HyperPipeline(Query& q, size_t thread_id) : IPipeline(q, thread_id) {} 
};

#endif