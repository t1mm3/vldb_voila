#include "runtime_struct.hpp"
#include "runtime_framework.hpp"
#include "runtime_memory.hpp"
#include <tbb/tbb.h>
#include <cstring>

inline static u64
next_power_2(u64 x)
{
	u64 power = 1;
	while (power < x) {
    	power*=2;
	}
	return power;
}


template<size_t NUM_VARIANTS>
struct Adaptive {
private:
	size_t prof_times[NUM_VARIANTS];
	static constexpr size_t min_samples = 2;
	int min_time;
	size_t runs;

	static constexpr size_t kMultiplier = 1;

	void reset() {
		for (size_t i=0; i<NUM_VARIANTS; i++) {
			prof_times[i] = 0;
		}
		min_time = -1;
		runs = 0;
	}
public:
	Adaptive() {
		static_assert(NUM_VARIANTS > 0, "Must have more than one flavor");

		reset();
	}

	struct Context {
		const size_t variant;
		const size_t num_values;
		Adaptive& adapt;
		size_t start;

		Context(Adaptive& a, size_t values = 1)
			 : variant(a.chose_variant()), num_values(values), adapt(a)
		{
			start = rdtsc();
		} 

		~Context() {
			size_t t = rdtsc() - start;
			t *= kMultiplier;
			t /= num_values;

			// printf("var=%d t=%d num=%d\n", variant, t, num_values);
			adapt.set_time(variant, t);
		}

		size_t get_variant() const {
			return variant;
		}
	};

	size_t chose_variant() {
		runs++;

		if (runs > NUM_VARIANTS*32) {
#if 0
			int min_idx = 0;
			size_t min_val = prof_times[0];
			int max_idx = 0;
			size_t max_val = prof_times[0];
			for (size_t i=1; i<NUM_VARIANTS; i++) {
				auto val = prof_times[i];
				if (val < min_val) {
					min_val = val;
					min_idx = i;
				}
				if (val > max_val) {
					max_val = val;
					max_idx = i;
				}
			}

			for (size_t i=0; i<NUM_VARIANTS; i++) {
				const char* msg = "";

				if (i == min_idx) {
					msg = "(min)";
				}
				if (i == max_idx) {
					msg = "(max)";
				}
				printf("%d: %d %s\n", i, prof_times[i],
					msg);
			}
#endif
			reset();
		}

		if (min_time >= 0) {
			ASSERT(min_time < (int)NUM_VARIANTS);
			return min_time;
		}

		if (runs <= NUM_VARIANTS*min_samples) {
			return runs % NUM_VARIANTS;
		}

		size_t min = prof_times[0];
		min_time = 0;
		for (size_t i=1; i<NUM_VARIANTS; i++) {
			if (prof_times[i] < min) {
				min = prof_times[i];
				min_time = i;
			}
		}

		if (min) {
			// printf("optimum %d\n", min_time);
		} else {
			min_time = -1;
		}


		ASSERT(min_time < (int)NUM_VARIANTS);
		return min_time;
	}

	void set_time(size_t var, size_t time) {
		if (prof_times[var]) {
			prof_times[var] = (prof_times[var] + ((min_samples-1) * time)) / min_samples;
		} else {
			prof_times[var] = time;
		}
	}
};



template<typename T>
void fetch(T* RESTRICT res, T* RESTRICT data, size_t stride,
	size_t num, size_t offset)
{
#define A(off, STRIDE) res[i] = data[(i+offset+off) * STRIDE];

	if (stride == 1) {
		for (size_t i=0; i<num; i++) {
			A(0, 1);
		}
	} else {
		for (size_t i=0; i<num; i++) {
			A(0, stride);
		}
	}

#undef A
}

#ifdef __AVX512F__
#include <immintrin.h>
#include <emmintrin.h>
#include <xmmintrin.h>
#endif

#define PREFETCH_CONST2 0
#ifdef __AVX512F__
#define STRUCT_PREFETCH(ptr, rw, tmp) _mm_prefetch((const void*)ptr, _MM_HINT_ET1)
#else
#define STRUCT_PREFETCH(ptr, rw, tmp)
#endif
#define STRUCT_PREFETCH_READ_ONLY(ptr ) PREFETCH_DATA0(ptr)

template<bool PARALLEL, size_t NEXT_STRIDE, size_t HASH_STRIDE, size_t WIDTH,
	int PREFETCH, int UNROLL, int PRE_COMP, int PRE_REP, int IMV>
 static NOINLINE void
__reinsert_hash_buckets(void** RESTRICT buckets,
	u64* RESTRICT hashs, size_t _hash_stride,
	void** RESTRICT nexts, size_t _next_stride,
	u64 mask, size_t offset, char* RESTRICT base_ptr, size_t _width,
	size_t num, size_t start, void*** RESTRICT tmp_ptr,
	void*** RESTRICT tmp_next, void** RESTRICT tmp_b,
	void** RESTRICT tmp_old, char* RESTRICT tmp_ok)
{
	u64 _k=start;

	(void)tmp_ptr;
	(void)tmp_next;
	(void)tmp_b;
	(void)tmp_old;
	(void)tmp_ok;

	auto par_insert_pre = [&] (void** next, void* b, void** ptr, void* old) -> bool {
		size_t it = 0;
		DBG_ASSERT(old != b);
		it++;
		*next = old;

		auto atomic = (std::atomic<void*>*)ptr;
		return atomic->compare_exchange_weak(old, b,
			std::memory_order_relaxed, std::memory_order_relaxed);

		// return __sync_bool_compare_and_swap(ptr, old, b);
	};

	auto par_insert_full = [&] (void** next, void* b, void** ptr) {
		size_t it = 0;
		while (!par_insert_pre(next, b, ptr, *ptr)) {
			it++;
		}
	};


	auto seq_insert = [&] (void** next, auto b, void** ptr) {
		DBG_ASSERT(*ptr != b);
		*next = *ptr;
		*ptr = b;
	};

	const size_t next_stride = NEXT_STRIDE ? NEXT_STRIDE : _next_stride;
	const size_t hash_stride = HASH_STRIDE ? HASH_STRIDE : _hash_stride;
	const size_t width = WIDTH ? WIDTH : _width;

#ifdef __AVX512F__
	if (IMV) {
		size_t off = start + offset;
		size_t n = num-start;

	constexpr size_t PAR = 8;
	constexpr size_t CONC = 8;

	int running = CONC;

	struct {
		__m512i vbs;
		__m512i vnexts;
		__m512i vptrs;
		__m512i vhptrs;
		int stage;
	} all_states[CONC];

	for (int k=0; k<CONC; k++) {
		all_states[k].stage = 0;
	}

	int current = 0;
	size_t consumed = 0;
	const auto vids = _mm512_set_epi64(7, 6, 5, 4, 3, 2, 1, 0);
	const auto vbase = _mm512_set1_epi64((u64)base_ptr);
	const auto vbuckets = _mm512_set1_epi64((u64)buckets);
	const auto vhashs = _mm512_set1_epi64((u64)hashs);
	const auto vnexts = _mm512_set1_epi64((u64)nexts);
	const auto vmask = _mm512_set1_epi64(mask);

	while (1) {
		auto& state = all_states[current % CONC];
		switch (state.stage) {
		case 0: {
			// inflow
			if (consumed + PAR >= n) {
				state.stage = 3;
				current++;
				break;
			}
			auto is = _mm512_add_epi64(vids,
				_mm512_set1_epi64(off + consumed));
			consumed += PAR;

			state.vhptrs = _mm512_add_epi64(
					vhashs,
					avx512_mulconst_epi64<HASH_STRIDE*8>(is, hash_stride*8)
				);

			state.vnexts = _mm512_add_epi64(vnexts,
					avx512_mulconst_epi64<NEXT_STRIDE*8>(is, next_stride*8)
				);

			state.vbs = _mm512_add_epi64(vbase, 
				avx512_mulconst_epi64<WIDTH>(is, width));

			if (hash_stride <= 2) {
				for (size_t k=0; k<PAR; k+=4) {
					STRUCT_PREFETCH(state.vhptrs[k], 0, PREFETCH_CONST2);
				}
			} else if (hash_stride == 4) {
				for (size_t k=0; k<PAR; k+=2) {
					STRUCT_PREFETCH(state.vhptrs[k], 0, PREFETCH_CONST2);
				}
			} else {
				for (size_t k=0; k<PAR; k++) {
					STRUCT_PREFETCH(state.vhptrs[k], 0, PREFETCH_CONST2);
				}
			}

			state.stage = 1;
			current++;
			break; }

		case 1: {
			auto hs = _mm512_i64gather_epi64(state.vhptrs,
				nullptr, 1);

			state.vptrs = _mm512_add_epi64(
				vbuckets,
				_mm512_slli_epi64(
					_mm512_and_epi64(hs, vmask),
					3));

			for (size_t k=0; k<PAR; k++) {
				STRUCT_PREFETCH(state.vptrs[k], PREFETCH - 1, PREFETCH_CONST2);
				// STRUCT_PREFETCH(state.vnexts[k], PREFETCH - 1, PREFETCH_CONST2);
			}

			state.stage = 2;
			current++;
			break; }

		case 2: {
			// insert 
			int fail = 0;

#if 1
			auto olds = _mm512_i64gather_epi64(
				state.vptrs, nullptr, 1);
			_mm512_i64scatter_epi64(nullptr, state.vnexts, olds, 1);
			for (size_t k=0; k<PAR; k++) {
				void* old = (void*) olds[k];
#else
			for (size_t k=0; k<PAR; k++) {
				void* old = *(void**)state.vptrs[k];
#endif
				auto atomic = (std::atomic<void*>*)state.vptrs[k];
				auto success = atomic->compare_exchange_weak(old,
						(void*)state.vbs[k],
						std::memory_order_relaxed,
						std::memory_order_relaxed);

#if 1
				if (!success) {
					fail |= (1 << k);
				}
#else
				fail |= (((~success) & 1) << k);
#endif
			}

			if (fail) {
				for (size_t k=0; k<PAR; k++) {
					if (fail & (1 << k)) {
						par_insert_full((void**)state.vnexts[k], (void*)state.vbs[k], (void**)state.vptrs[k]);
					}
				}
			}
			current++;
			state.stage = 0;
			break; }

		case 3:
			running--;
			state.stage = 4;
			current++;
			break;

		case 4:
			if (!running) {
				goto out;
			}
			current++;
			break;
		}
	}

	out:

#undef A

		for (;consumed<n; consumed++) {
			u64 i0 = off+consumed;
			u64 h = hashs[i0 * hash_stride];
			auto idx = h & mask;
			auto b = base_ptr + (i0 * width);
			auto ptr = &buckets[idx];
			auto next = &nexts[i0 * next_stride];
			par_insert_full(next, b, ptr);
		}
		return;
	}
#endif

	STRUCT_PREFETCH_READ_ONLY(&tmp_ptr[0]);
	STRUCT_PREFETCH_READ_ONLY(&tmp_next[0]);

	if (PRE_COMP) {
		size_t off = start + offset;
		size_t n = num-start;

#define PRE_COMPUTE(pos) {\
				const u64 i0 = i+off+pos; \
				const auto h0 = hashs[i0 * hash_stride]; \
				u64 idx0 = h0 & mask; \
				tmp_old[i] = (void*)idx0; \
				tmp_b[i] = base_ptr + (i0 * width); \
				tmp_ptr[i] = &buckets[idx0]; \
				tmp_next[i] = &nexts[i0 * next_stride]; \
			}

		size_t i=0;


		// prefetch first 8
		if (n > PRE_COMP) {
#define A(pos) \
	PRE_COMPUTE(pos); \
	if (PREFETCH >= 0 && PREFETCH) { \
		STRUCT_PREFETCH(tmp_ptr[i+pos], PREFETCH - 1, PREFETCH_CONST2); \
		/* STRUCT_PREFETCH(tmp_next[i+pos], PREFETCH - 1, PREFETCH_CONST2); */ \
	}
			constexpr size_t M = std::min(PRE_COMP, 32);
			for (; i<M; i++) {
				A(0);
			}
#undef A
		}

		for (; i<n; i++) {
			PRE_COMPUTE(0)
		}
#undef PRE_COMPUTE
	}


#define PREF_AHEAD(AHEAD, N) (AHEAD && ((PRE_REP && PRE_COMP >= N) || (!PRE_REP && PRE_COMP == N)))

#define PROLOGUE2(pos, AHEAD) \
		u64 k##pos=pos+_k; \
		u64 i##pos = offset+k##pos; \
		u64 idx##pos; \
		void* RESTRICT b##pos; \
		void** RESTRICT ptr##pos; \
		void** RESTRICT next##pos; \
		if (PRE_COMP) { \
			const auto z = k##pos-start; \
			idx##pos = (u64)tmp_old[z]; \
			b##pos = tmp_b[z]; \
			ptr##pos = tmp_ptr[z]; \
			next##pos = tmp_next[z]; \
			if (PREFETCH >= 0 && PREFETCH) { \
				if (true || pos == 0) { \
					STRUCT_PREFETCH_READ_ONLY(&tmp_ptr[z+PRE_COMP*2]); \
					STRUCT_PREFETCH_READ_ONLY(&tmp_next[z+PRE_COMP*2]); \
				} \
				if (PREF_AHEAD(AHEAD, 8)) { \
					STRUCT_PREFETCH(tmp_ptr[z+8], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+8], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 16)) { \
					STRUCT_PREFETCH(tmp_ptr[z+16], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+16], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 24)) { \
					STRUCT_PREFETCH(tmp_ptr[z+24], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+24], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 32)) { \
					STRUCT_PREFETCH(tmp_ptr[z+32], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+32], PREFETCH - 1, PREFETCH_CONST2); */  \
				} \
				if (PREF_AHEAD(AHEAD, 40)) { \
					STRUCT_PREFETCH(tmp_ptr[z+40], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+40], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 48)) { \
					STRUCT_PREFETCH(tmp_ptr[z+48], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+48], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 64)) { \
					STRUCT_PREFETCH(tmp_ptr[z+64], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+64], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PREF_AHEAD(AHEAD, 128)) { \
					STRUCT_PREFETCH(tmp_ptr[z+128], PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(tmp_next[z+128], PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
				if (PRE_COMP == 0) { \
					STRUCT_PREFETCH(ptr##pos, PREFETCH - 1, PREFETCH_CONST2); \
					/* STRUCT_PREFETCH(next##pos, PREFETCH - 1, PREFETCH_CONST2); */ \
				} \
			} \
		} else { \
			u64 h##pos = hashs[i##pos * hash_stride]; \
			idx##pos = h##pos & mask; \
			b##pos = base_ptr + (i##pos * width); \
			ptr##pos = &buckets[idx##pos]; \
			next##pos = &nexts[i##pos * next_stride]; \
			if (PREFETCH >= 0 && PREFETCH) { \
				STRUCT_PREFETCH(ptr##pos, PREFETCH - 1, PREFETCH_CONST2); \
				STRUCT_PREFETCH(next##pos, PREFETCH - 1, PREFETCH_CONST2); \
			} \
		}


	if (PARALLEL) {
#define D(pos, ahead) PROLOGUE2(pos, ahead); \
		bool ok##pos;

#define E(pos) void* old##pos; old##pos = *ptr##pos;
#define A(pos) ok##pos = par_insert_pre(next##pos, b##pos, ptr##pos, old##pos);

#define B(pos) if (!ok##pos) { \
		par_insert_full(next##pos, b##pos, ptr##pos); \
	}
	
	if (UNROLL == 8) {
		// parallel
		constexpr size_t kMargin = std::max(8, PRE_COMP);
		for (; _k+kMargin<num; _k+=8) {
			D(0, true)
			D(1, true)
			D(2, true)
			D(3, true)
			D(4, true)
			D(5, true)
			D(6, true)
			D(7, true)

			E(0)
			E(1)
			E(2)
			E(3)
			E(4)
			E(5)
			E(6)
			E(7)

			A(0)
			A(1)
			A(2)
			A(3)
			A(4)
			A(5)
			A(6)
			A(7)

			if (UNLIKELY(!(ok0 & ok1 & ok2 & ok3 & ok4 & ok5 & ok6 & ok7))) {
				B(0)
				B(1)
				B(2)
				B(3)
				B(4)
				B(5)
				B(6)
				B(7)
			}	
		}

		// no prefetch ahead
		for (; _k+8<num; _k+=8) {
			D(0, false)
			D(1, false)
			D(2, false)
			D(3, false)
			D(4, false)
			D(5, false)
			D(6, false)
			D(7, false)

			E(0)
			E(1)
			E(2)
			E(3)
			E(4)
			E(5)
			E(6)
			E(7)

			A(0)
			A(1)
			A(2)
			A(3)
			A(4)
			A(5)
			A(6)
			A(7)

			if (UNLIKELY(!(ok0 & ok1 & ok2 & ok3 & ok4 & ok5 & ok6 & ok7))) {
				B(0)
				B(1)
				B(2)
				B(3)
				B(4)
				B(5)
				B(6)
				B(7)
			}	
		}
	}

#undef D
#undef B
#undef A
	} else {
		// sequential
#define A(pos) { \
		PROLOGUE2(pos, false) \
		seq_insert(next##pos, b##pos, ptr##pos); \
	} \

		for (; _k+8<num; _k+=8) {
			A(0)
			A(1)
			A(2)
			A(3)
			A(4)
			A(5)
			A(6)
			A(7)
		}
#undef A
	}

#define A(pos) { \
		PROLOGUE2(pos, false); \
		if (PARALLEL) { \
			par_insert_full(next##pos, b##pos, ptr##pos); \
		} else { \
			seq_insert(next##pos, b##pos, ptr##pos); \
		} \
	}

	for (; _k<num; _k++) {
		A(0)
	}
#undef A
}

struct ReinsertContext {	
	void*** tmp_ptr;
	void*** tmp_next;
	void** tmp_b;
	void** tmp_old;
	char* tmp_ok; 
}; 


static constexpr size_t kNumAdaptiveChoices = 4;
static constexpr size_t kChunkSize = 512;

template<bool PARALLEL, size_t NEXT_STRIDE, size_t HASH_STRIDE, size_t WIDTH,
	typename T>
static void
_reinsert_hash_buckets(void** RESTRICT buckets,
	u64* RESTRICT hashs, size_t _hash_stride,
	void** RESTRICT nexts, size_t _next_stride,
	u64 mask, size_t offset, char* RESTRICT base_ptr, size_t _width,
	size_t total_num, T& adaptive_framework, ReinsertContext& ctx)
{
#define CALL(START, NUM, PREFETCH, UNROLL, PRE_COMP, PRE_REP, IMV) \
	__reinsert_hash_buckets<PARALLEL, NEXT_STRIDE, HASH_STRIDE, WIDTH, \
		PREFETCH, UNROLL, PRE_COMP, PRE_REP, IMV>( \
	buckets, hashs, _hash_stride, \
	nexts, _next_stride, mask, offset, base_ptr, _width, NUM, START, \
	ctx.tmp_ptr, ctx.tmp_next, ctx.tmp_b, ctx.tmp_old, ctx.tmp_ok)

	
	for (size_t i=0; i<total_num; i+=kChunkSize) {
		const size_t num = std::min(kChunkSize, total_num-i);

		typename T::Context adapt(adaptive_framework, num);
		const size_t flavor = adapt.get_variant();

		const size_t end = i+num;

		// printf("start %d num %d of total %d, try %d\n", i, num, total_num, flavor);

		switch (flavor) {
#if 1
		case 1:	CALL(i, end, 1, 8, 8, 0, 0);	break;
		case 2: CALL(i, end, 0, 8, 0, 0, 1);	break;
		case 3:	CALL(i, end, 1, 8, 32, 0, 0);	break;
		// case 4:	CALL(i, end, 1, 8, 32, 0, 0);	break;
		// case 5:	CALL(i, end, 1, 8, 64, 0, 0);	break;
		// case 7: CALL(i, end, 1, 8, 0, 0, 0);	break;
#endif
		default:CALL(i, end, 0, 8, 0, 0, 0);	break;
		}
	}
}


template<bool parallel, typename T> static void
reinsert_hash_buckets(void** RESTRICT buckets,
	u64* RESTRICT hashs, size_t hash_stride,
	void** RESTRICT nexts, size_t next_stride,
	u64 mask, size_t offset, char* base_ptr, size_t width,
	size_t num, T& adaptive_framework, ReinsertContext& ctx)
{
	switch (next_stride) {
#define C(NEXT_STRIDE, HASH_STRIDE, WIDTH) \
			case WIDTH: \
				_reinsert_hash_buckets<parallel, NEXT_STRIDE, HASH_STRIDE, WIDTH>( \
				buckets, hashs, HASH_STRIDE, nexts, \
				NEXT_STRIDE, mask, offset, base_ptr, WIDTH, \
				num, adaptive_framework, ctx); \
				break; \


#define B(NEXT_STRIDE, HASH_STRIDE) \
			case HASH_STRIDE: \
				switch (width) { \
				/* C(NEXT_STRIDE, HASH_STRIDE, 2); */ \
				/* C(NEXT_STRIDE, HASH_STRIDE, 4); */ \
				/* C(NEXT_STRIDE, HASH_STRIDE, 8); */ \
				C(NEXT_STRIDE, HASH_STRIDE, 16); \
				C(NEXT_STRIDE, HASH_STRIDE, 32); \
				/* C(NEXT_STRIDE, HASH_STRIDE, 48); */ \
				/* C(NEXT_STRIDE, HASH_STRIDE, 64); */ \
				/* C(NEXT_STRIDE, HASH_STRIDE, 80); */ \
				default: \
					_reinsert_hash_buckets<parallel, NEXT_STRIDE, HASH_STRIDE, 0>( \
					buckets, hashs, HASH_STRIDE, nexts, \
					NEXT_STRIDE, mask, offset, base_ptr, width, \
					num, adaptive_framework, ctx); \
					break; \
				} \
				break;

#define A(NEXT_STRIDE) \
		case NEXT_STRIDE: \
			switch (hash_stride) { \
			B(NEXT_STRIDE, 2); \
			B(NEXT_STRIDE, 4); \
			/* B(NEXT_STRIDE, 6); */ \
			B(NEXT_STRIDE, 8); \
			/* B(NEXT_STRIDE, 10); */ \
			default: \
				_reinsert_hash_buckets<parallel, NEXT_STRIDE, 0, 0>( \
					buckets, hashs, hash_stride, nexts, \
					NEXT_STRIDE, mask, offset, base_ptr, width, num, \
					adaptive_framework, ctx); \
				break; \
			} \
			break;


	A(2)
	A(4)
	/* A(6) */
	A(8)
	/* A(10) */

		default:
			_reinsert_hash_buckets<parallel,0,0,0>(
				buckets, hashs, hash_stride, nexts,
				next_stride, mask, offset, base_ptr, width, num,
				adaptive_framework, ctx);
			break;
	}

#undef A
#undef B
}

template<typename T>
static void
reinsert_hash_buckets_parallel(void** RESTRICT buckets,
	u64* RESTRICT hashs, size_t hash_stride,
	void** RESTRICT nexts, size_t next_stride,
	u64 mask, size_t offset, char* base_ptr, size_t width,
	size_t num, T& adaptive_framework, ReinsertContext& ctx)
{
	reinsert_hash_buckets<true, T>(buckets, hashs, hash_stride,
		nexts, next_stride, mask, offset, base_ptr, width, num,
		adaptive_framework, ctx);
}

template<typename T>
static void
reinsert_hash_buckets_sequential(void** RESTRICT buckets,
	u64* RESTRICT hashs, size_t hash_stride,
	void** RESTRICT nexts, size_t next_stride,
	u64 mask, size_t offset, char* base_ptr, size_t width,
	size_t num, T& adaptive_framework, ReinsertContext& ctx)
{
	reinsert_hash_buckets<false, T>(buckets, hashs, hash_stride,
		nexts, next_stride, mask, offset, base_ptr, width, num,
		adaptive_framework, ctx);
}


inline static
size_t calc_num_buckets(QueryConfig& config, size_t count, size_t mul = 1)
{
	// return 2;
	size_t bucket_count = count * mul;
	auto bmax = [&] (const auto& v) {
		if (v > bucket_count) {
			bucket_count = v;
		}
	};

	bmax(config.min_bucket_count);
	bmax(config.vector_size * 2);

	size_t nrEntries = bucket_count;

	const auto loadFactor = 0.7;
	size_t exp = 64 - __builtin_clzll(nrEntries);
	assert(exp < sizeof(hash_t) * 8);
	if (((size_t) 1 << exp) < nrEntries / loadFactor)
		exp++;
	size_t capacity = ((size_t) 1) << exp;

	return capacity;
}

Morsel::Morsel() {
	_num = -1;
	ASSERT(!IS_VALID(*this));
}

#include "common/runtime/Database.hpp"

using namespace runtime;

IBaseColumn::IBaseColumn(Query& query, const std::string& tbl,
	const std::string& col, size_t max_len, int varlen)
 : max_len(max_len), varlen(varlen)
{
	std::string t(tbl);
	std::string c(col);

	auto& rel = query.config.db[t];
	auto& attr = rel[c];

	data = attr.data();

	if (varlen) {
		ASSERT(attr.varchar_data.size() > 0);
		data = &attr.varchar_data[0];
	}
	size = rel.nrTuples;
}

IBaseTable::IBaseTable(const char* dbg_name, Query& query)
 : query(query)
{
	reset();
	query.add_resetable(this);
}

IBaseTable::~IBaseTable()
{
}

void
IBaseTable::get_scan_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file, int dbg_line)
{
	pos_t morsel_size = query.config.morsel_size;

	morsel.init(morsel_offset.fetch_add(morsel_size), -1);

	if (morsel._offset >= capacity) {
		LOG_TRACE("get_scan_morsel: done at offset=%lld capacity=%lld\n",
			morsel._offset, capacity);
		return;
	}

	morsel._num = std::min(morsel_size, capacity - morsel._offset);

	LOG_TRACE("get_scan_morsel: offset=%lld num=%lld @ %s:%d\n",
		morsel._offset, morsel._num, dbg_file, dbg_line);
}

void
LogicalMasterTable::add(ITable& table)
{
	std::lock_guard<std::mutex> lock(mutex);

	tables.push_back(&table);
	LOG_TRACE("LogicalMasterTable: adding table %p num_tables(post) %lld\n",
		&table, tables.size());
}

void
LogicalMasterTable::get_read_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file, int dbg_line)
{
	while (1) {
		if (ctx.index >= tables.size()) {
			LOG_TRACE("LogicalMasterTable::get_read_morsel: part %lld out\n",
				ctx.pipeline.thread_id);
			ASSERT(ctx.index == tables.size());
			morsel.init(-1, -1);
			break;
		}

		const size_t index = ctx.index;
		ASSERT(index < tables.size());
		auto t = tables[index];
		ASSERT(t);
		auto partition = t->m_flush_partitions[ctx.pipeline.thread_id];
		ASSERT(partition);

		Block* blk = (Block*)ctx.last_buffer;
		const bool generate_range = blk && blk->num > 0;

		if (generate_range) {
			// output non-empty range
			morsel.init(0, blk->num, blk->data);
			LOG_TRACE("LogicalMasterTable::get_read_morsel: "
				"part %lld table %lld "
				"morsel_offset %ld morsel_num %lld morsel_data %p "
				"block num %lld block cap %lld block width %lld"
				"\n",
				ctx.pipeline.thread_id, index, morsel._offset,
				morsel._num, morsel.data, blk->num, blk->capacity,
				blk->width);
		}

		// go to next buffer
		if (blk) {
			ctx.last_buffer = blk->next;
		} else {
			blk = partition->head;
			ctx.last_buffer = blk;
		}

		if (!ctx.last_buffer) {
			ctx.index++;
			LOG_TRACE("LogicalMasterTable::get_read_morsel: "
				"part %lld set table to %lld\n",
				ctx.pipeline.thread_id, ctx.index);
		}

		if (generate_range) {
			// ASSERT(morsel._num <= 4);
			break;
		}
	};
}

















Block::Block(size_t _width, size_t _capacity, Block* _prev, Block* _next)
{
	prev = _prev;
	next = _next;

	width = _width;
	capacity = _capacity;
	num = 0;

	data = (char*) calloc(1, width*capacity);
}

Block::~Block()
{
	free(data);
}



BlockFactory::BlockFactory(size_t width, size_t capacity) noexcept
 : width(width), capacity(capacity)
{

}

Block*
BlockFactory::new_block(Block* prev, Block* next)
{
	return new Block(width, capacity, prev, next);
}




BlockedSpace::BlockedSpace(size_t width, size_t capacity) noexcept
 : head(nullptr), tail(nullptr), factory(width, capacity)
{
}

Block*
BlockedSpace::new_block_at_end()
{
	Block* b = factory.new_block(tail, nullptr);

	LOG_TRACE("new_block_at_end: %p cap=%lld width=%lld\n",
		b->data, b->capacity, b->width);
	if (!head) {
		head = b;
	}
	tail = b;
	return b;
}

void
BlockedSpace::reset()
{
	// dealloc space
	Block* b = head;
	while (b) {
		Block* next = b->next;
		delete b;
		b = next;
	}

	head = nullptr;
	tail = nullptr;
}

size_t
BlockedSpace::size() const
{
	size_t n=0;

	for_each([&] (auto blk) {
		n += blk->size();
	});
	return n;
}

void
BlockedSpace::partition_data(BlockedSpace** partitions, size_t num_partitions,
	u64* hashes, size_t hash_stride, char* data, size_t width,
	size_t total_num)
{
	const size_t vsize = 1024;

	u64 tmp_hashes[vsize];
	char* part_buf[num_partitions];
	size_t part_num[num_partitions];
	Block* part_blk[num_partitions];

	// make sure everything fits & set buffers
	for (size_t i=0; i<num_partitions; i++) {
		Block* b = partitions[i]->append(total_num);

		part_blk[i] = b;
		part_buf[i] = b->data + (b->num * width);
		part_num[i] = b->num;
	}

	LOG_DEBUG("partition data %p total %lld\n", data, total_num);

	// partition
	for (size_t offset=0; offset < total_num; offset += vsize) {
		const size_t num = std::min(total_num - offset, vsize);

		// fetch hashes
		fetch<u64>(tmp_hashes, hashes, hash_stride, num, offset);

		for (size_t i=0; i<num; i++) {
			ASSERT(tmp_hashes[i] != 0);
		}

		// partition
		runtime_partition(part_num, part_buf, num_partitions, data, width,
			tmp_hashes, num, offset);
	}

	// maintain counts
	for (size_t i=0; i<num_partitions; i++) {
		part_blk[i]->num += part_num[i];
	}
}

void
BlockedSpace::partition(BlockedSpace** partitions, size_t num_partitions,
	size_t hash_offset, size_t hash_stride) const
{
	const size_t width = factory.width;

	size_t num = 0;

	for_each([&] (auto block) {
		num++;
		char* blk_data = block->data;
		u64* blk_hashes = (u64*)(blk_data + hash_offset);
		partition_data(partitions, num_partitions, blk_hashes, hash_stride,
			blk_data, width, block->num);
	});

	ASSERT(!num == !head);
}


BlockedSpace::~BlockedSpace() {
	reset();
}




ITable::ITable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
	size_t row_width, bool fully_thread_local, bool flush_to_part)
 : dbg_name(dbg_name), query(q), m_fully_thread_local(fully_thread_local),
 		m_flush_to_partitions(flush_to_part), m_row_width(row_width),
 		m_master_table(master_table)
 {
	size_t num_write_parts = query.config.num_threads;

	if (m_fully_thread_local) {
		num_write_parts = 1;
	}

	auto new_space = [&] () {
		return new BlockedSpace(m_row_width, m_block_capacity);
	};

	for (size_t t=0; t<num_write_parts; t++) {
		m_write_partitions.push_back(new_space());
	}

	if (m_flush_to_partitions) {
		for (size_t t=0; t<q.config.num_threads; t++) {
			m_flush_partitions.push_back(new_space());
		}
	}

	remove_hash_index();

	q.add_resetable(this);

	if (m_master_table) {
		m_master_table->add(*this);
	}
}

ITable::~ITable()
{
	ForEachSpace([&] (auto space) {
		delete space;
	});
}

void
ITable::init()
{
	reset_pointers();
}

void
ITable::reset_pointers()
{
	ASSERT(false && "overwritten");
}

void
ITable::reset()
{
	remove_hash_index();

	ForEachSpace([&] (auto space) {
		space->reset();
	});

	build_index(true, nullptr);
}

template<typename T>
static void
create_hash_index_for_block(void** buckets, u64 mask, const Block& block,
	size_t hash_offset, size_t hash_stride, size_t next_offset, size_t next_stride,
	bool parallel, T& adaptive_framework, ReinsertContext& ctx)
{
	char* blk_data = block.data;
	const size_t num = block.num;
	const size_t offset = 0;
	const size_t width = block.width;

	u64* blk_hashes = (u64*)(blk_data + hash_offset);
	void** blk_nexts = (void**)(blk_data + next_offset);

	LOG_TRACE("create_hash_index_for_block: buckets=%p hashes=%p nexts=%p num=%lld "
		"offset=%lld width=%lld\n",
		buckets, blk_hashes, blk_nexts, num, offset, width);

	static_assert(sizeof(u64) == sizeof(void*), "64-bit pointers required");

	if (!parallel) {
		// FIXME: revert to sequential algorithm
		reinsert_hash_buckets_sequential<T>(buckets, blk_hashes, hash_stride,
			blk_nexts, next_stride, mask, offset, blk_data, width, num,
			adaptive_framework, ctx);
	} else {
		reinsert_hash_buckets_parallel<T>(buckets, blk_hashes, hash_stride,
			blk_nexts, next_stride, mask, offset, blk_data, width, num,
			adaptive_framework, ctx);
	}
}

void
ITable::create_hash_index_handle_space(void** buckets, u64 mask,
	const size_t* thread_id, bool parallel, const BlockedSpace* space)
{
	// not used
	const int kVectorSize = kChunkSize;
	void** chk_ptr[kVectorSize];
	void** chk_next[kVectorSize];
	void* chk_b[kVectorSize];
	void* chk_old[kVectorSize];
	char chk_ok[kVectorSize];

	ReinsertContext ctx { &chk_ptr[0], &chk_next[0],
		&chk_b[0], &chk_old[0], &chk_ok[0]};

	#define A() \
		space->for_each([&] (Block* block) { \
			LOG_TRACE("create_hash_index_for_block: %p " \
				"space=%p blk=%p blk_base=%p num=%lld width=%lld\n", \
				parallel ? "PARALLEL" : "SEQUENTIAL", \
				space, block, block->data, block->num, block->width); \
			\
			create_hash_index_for_block(buckets, mask, *block, \
				hash_offset, hash_stride, next_offset, next_stride, \
				parallel, adaptive_framework, ctx); \
		});

	if (query.config.adaptive_ht_chaining) {
		Adaptive<kNumAdaptiveChoices> adaptive_framework;
		A();
	} else {
		Adaptive<1> adaptive_framework;		
		A();
	}
}

void
ITable::create_hash_index(void** buckets, u64 mask, const size_t* thread_id)
{
	LOG_DEBUG("create_hash_index(mask=%p)\n", mask);
	ASSERT(mask != 0);
	ASSERT(hash_stride > 0 && hash_offset > 0);
	ASSERT(next_stride > 0 && next_offset > 0);

	bool parallel = !m_fully_thread_local && query.config.num_threads > 0;

	if (parallel && thread_id) {
		size_t tid = *thread_id;
		ASSERT(tid < m_write_partitions.size());
		create_hash_index_handle_space(buckets, mask, &tid,
			parallel, m_write_partitions[tid]);
	} else {
		ForEachSpace([&] (auto& space) {
			create_hash_index_handle_space(buckets, mask, thread_id,
				parallel, space);
		});
	}
}

void
ITable::remove_hash_index()
{
	if (!hash_index_head) {
		return;
	}
	hash_index_head = nullptr;

	if (hash_index_buffer) {
		hash_index_buffer->free();
	}
	hash_index_mask = 0;
	hash_index_capacity = 0;
	hash_index_tuple_counter_seq = 0;
	hash_index_tuple_counter_par = 0;
}

bool
ITable::build_index(bool force, IPipeline* pipeline)
{
	const size_t count = get_non_flushed_table_count();
	const size_t new_num_buckets = calc_num_buckets(query.config, count);
	const u64 mask = new_num_buckets - 1;

	// try to not rebuild needlessly
	if (!force) {
		if (hash_index_head && mask == hash_index_mask) {
			// re-use hash index
			return false;
		}
	}


	{
		std::lock_guard<std::mutex> lock(mutex);
		if (!hash_index_head || mask != hash_index_mask) {
			// throw away old hash index
			remove_hash_index();

			LOG_DEBUG("build_index(%p, %s): count %d\n", this, dbg_name, count);

			// create new hash index
			hash_index_mask = mask;
			hash_index_capacity = new_num_buckets;
			ASSERT(new_num_buckets > 0);

			if (!hash_index_buffer) {
				hash_index_buffer = new LargeBuffer();
			}
			hash_index_buffer->alloc(sizeof(void*) * new_num_buckets);
			hash_index_head = hash_index_buffer->get<void*>();
			ASSERT(hash_index_head);
		}
	}

	create_hash_index(hash_index_head, hash_index_mask,
		pipeline ? &pipeline->thread_id : nullptr);

	return true;
}

void
ITable::flush2partitions()
{
	ASSERT(m_write_partitions.size() == 1);
	ASSERT(m_master_table);

	if (!m_flush_partitions.size()) {
		for (size_t i=0; i<query.config.num_threads; i++) {
			m_flush_partitions.push_back(new BlockedSpace(m_row_width, m_block_capacity));
		}
	}

	// flush
	BlockedSpace* bspace = m_write_partitions[0];
	bspace->partition(&m_flush_partitions[0], m_flush_partitions.size(),
		hash_offset, hash_stride);

	// remove flushed tuples from table
	bspace->reset();

	// delete hash index
	if (hash_index_head) {
		remove_hash_index();
	}
}

void
ITable::get_read_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file, int dbg_line)
{
	if (m_master_table) {
		ASSERT(m_flush_partitions.size() > 0);
		m_master_table->get_read_morsel(morsel, ctx);
		return;
	}

	ASSERT(m_fully_thread_local && "todo: implement shared scan");
	ASSERT(m_write_partitions.size() == 1);

	Block* blk = (Block*)ctx.last_buffer;

	if (!blk && ctx.index) {
		morsel.init(-1, -1);
		return;
	}

	if (!blk && !ctx.index) {
		blk = m_write_partitions[0]->head;
		ctx.index = 1;
	} else {
		blk = blk->next;
		ctx.index = 1;
	}

	if (!blk) {
		morsel.init(-1, -1);
		return;
	}

	ctx.last_buffer = blk;

	morsel.init(0, blk->num, blk->data);
}

ITable::ThreadView::ThreadView(ITable& t, IPipeline& pipeline)
 : table(t)
{
	size_t id = pipeline.thread_id;
	if (table.m_fully_thread_local) {
		id = 0;
	}

	ASSERT(id < table.m_write_partitions.size());
	space = table.m_write_partitions[id];
}




IHashTable::IHashTable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
	size_t row_width, bool fully_thread_local, bool flush_to_part)
 : ITable(dbg_name, q, master_table, row_width, fully_thread_local, flush_to_part)
{
}

void
IHashTable::reset()
{
	ITable::reset();
}

void
IHashTable::get_current_block_range(char** out_begin, char** out_end,
		size_t num) {
	const size_t width = get_row_width();

	ASSERT(m_fully_thread_local);
	ASSERT(m_write_partitions.size() == 1);
	auto part = m_write_partitions[0];
	Block* block = part->current();

	ASSERT(block && block->data);

	size_t offset = block->num;

	char* begin = block->data + (offset*width);
	char* end = begin + (num*width);

	*out_begin = begin;
	*out_end = end;
}

void trigger() {

}
