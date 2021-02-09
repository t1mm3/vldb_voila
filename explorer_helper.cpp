#include "explorer_helper.hpp"
#include <mutex>
#include "compiler.hpp"
#include "runtime_framework.hpp"

const std::vector<int> dom_fsms = {1, 2, 4, 8, 16, 32};
const std::vector<int> dom_prefetch = {0, 4, 3, 2, 1};

const uint64_t kGenEssential = 1 << 1;
const uint64_t kGenVectorSize = 1 << 2;

const std::vector<std::pair<std::string, uint64_t>> dom_comp = {
		{"scalar", kGenEssential},
#ifdef __AVX512F__
		{"avx512", kGenEssential},
#endif
		{"vector(256)", kGenVectorSize},
		{"vector(512)", kGenVectorSize},
		{"vector(1024)", kGenEssential | kGenVectorSize},
		{"vector(2048)", kGenVectorSize}
	};


ExploreMode g_explore_mode = ExploreMode::Unknown;
bool g_explore_dry = false;
int64_t g_explore_count_tries = 0;
int64_t g_explore_count_success = 0;
int64_t g_explore_sample_num = 0;

static std::unordered_map<GenBlendFlags, std::shared_ptr<std::vector<BlendConfig*>>> g_flavor_cache;

static std::vector<std::shared_ptr<BlendConfig>> allocated_blends;

static BlendConfig*
new_blend_config(const std::string& txt = "") {
	auto cfg = std::make_shared<BlendConfig>(txt);
	auto ptr = cfg.get();

	allocated_blends.emplace_back(std::move(cfg));
	return ptr;
}


std::shared_ptr<std::vector<BlendConfig*>>
_generate_blends(GenBlendFlags flags) {
	const bool use_cache = !(flags & kGenBlendNoCache);

	// cached
	auto cached = g_flavor_cache.find(flags);
	if (use_cache && cached != g_flavor_cache.end()) {
		return cached->second; 
	}

	std::vector<BlendConfig*> r;

	if (!(flags & kGenBlendOnlyBase)) {
		r.push_back(new_blend_config("NULL"));
	}

	for (const auto& fsms : dom_fsms) {
		for (const auto& pref : dom_prefetch) {
			if (flags & kGenBlendBinaryPrefetch) {
				if (pref != 0 && pref != 1) {
					continue;
				}

			}

			if ((flags & kGenBlendOnlyBase) && !pref && fsms != 1) {
				continue;
			}

			if (flags & kGenBlendOnlyEssentialFsm) {
				if (fsms > 8) {
					continue;
				}
			}

			for (const auto& comp_pair : dom_comp) {
				const auto comp_flags = std::get<1>(comp_pair);
				if (flags & kGenBlendOnlyEssentialComp) {
					if (!(comp_flags & kGenEssential)) {
						continue;
					}
				}

				const auto& comp = std::get<0>(comp_pair);

				auto b = new_blend_config();
				b->concurrent_fsms = fsms;
				b->prefetch = pref;
				b->computation_type = comp;
				r.push_back(b);
			}
		}
	}

	auto ptr = std::make_shared<std::vector<BlendConfig*>>(r);
	if (use_cache) {
		g_flavor_cache[flags] = ptr;
	}
	return ptr;
}


std::shared_ptr<std::vector<BlendConfig*>>
generate_blends(GenBlendFlags flags) {
	auto ptr = _generate_blends(flags);

	bool only_base = flags & kGenBlendOnlyBase;
#if 0
	printf("Gen Blends flags %d only_base %d\n",
		flags, only_base);
	for (auto& f : *ptr) {
		printf("%s\n", f->to_string().c_str());
	}
#endif
	return ptr;
}


std::vector<int>
get_prefetch_domain()
{
	return dom_prefetch;
}


std::vector<int>
get_fsms_domain()
{
	return dom_fsms;
}

std::vector<int>
get_expensive_pipeline_ids(const BenchmarkQuery& query)
{
	return get_most_expensive_pipeline_ids(query, 0);
}

std::vector<int>
get_most_expensive_pipeline_ids(const BenchmarkQuery& query, size_t k)
{
	std::vector<int> s;

	auto cost = query.expensive_pipelines;

	for (auto& kv : cost) {
		s.push_back(kv.first);
	}

	if (k) {
		std::sort(s.begin(), s.end(), [&] (int a, int b) {
	        return cost[a] > cost[b];   
	    });

	    while (s.size() > k) {
	    	s.pop_back();
	    }
	}
	return s;
}


#include <sys/file.h>

FdLockGuard::FdLockGuard(int fd) {
	fd_lock = fd;
	flock(fd_lock, LOCK_EX);
}

FdLockGuard::~FdLockGuard() {
	flock(fd_lock, LOCK_UN);
}