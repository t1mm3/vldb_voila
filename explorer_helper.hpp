#ifndef H_EXPLORER_HELPER
#define H_EXPLORER_HELPER

#include "voila.hpp"
#include "runtime_framework.hpp"

struct BenchmarkQuery;


enum ExploreMode {
	Unknown = 0,
	OnlyBase,
	PerPipelineBase,
	ExploreAll
};
extern ExploreMode g_explore_mode;
extern bool g_explore_dry;

// #Flavors explored
extern int64_t g_explore_count_tries;

extern int64_t g_explore_count_success;

// #Samples to be taken or 0, if no sampling
extern int64_t g_explore_sample_num;

typedef uint64_t GenBlendFlags;

static constexpr GenBlendFlags kGenBlendDefault = 0;
static constexpr GenBlendFlags kGenBlendBinaryPrefetch = 1 << 1;
static constexpr GenBlendFlags kGenBlendOnlyBase = 1 << 3;
static constexpr GenBlendFlags kGenBlendOnlyEssentialComp = 1 << 4;
static constexpr GenBlendFlags kGenBlendOnlyEssentialFsm = 1 << 5;
static constexpr GenBlendFlags kGenBlendNoCache = 1 << 6;


inline bool valid_base_to_other(const BlendConfig& base, const BlendConfig& other,
	GenBlendFlags flags)
{
	return base.concurrent_fsms == other.concurrent_fsms;
}

inline bool valid_base_to_other(const BlendConfig* base, const BlendConfig* other,
	GenBlendFlags flags)
{
	return valid_base_to_other(*base, *other, flags);	
}


std::shared_ptr<std::vector<BlendConfig*>>
generate_blends(GenBlendFlags flags);

std::vector<int>
get_expensive_pipeline_ids(const BenchmarkQuery& query);

std::vector<int>
get_most_expensive_pipeline_ids(const BenchmarkQuery& query, size_t k);

std::vector<int>
get_prefetch_domain();


void
backtrack(QueryConfig& qconf, BenchmarkQuery& query, size_t depth,
	const BlendConfig& default_blend, const std::string& thread_id);


struct FdLockGuard {
	FdLockGuard(int fd);
	~FdLockGuard();
private:
	int fd_lock;
};

#endif