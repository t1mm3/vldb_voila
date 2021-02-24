#include "runtime.hpp"

#include <iostream>
#include <cstring>

TypeCode
type_code_from_str(const char* str)
{
#define F(tpe, _) if (!strcmp(str, #tpe)) return TypeCode_##tpe;
		TYPE_EXPAND_ALL_TYPES(F, _)
#undef F
	return TypeCode_invalid;
}

size_t
type_width_bytes(TypeCode c)
{
	switch (c) {

#define F(tpe, _) case TypeCode_##tpe: return sizeof(tpe);
		TYPE_EXPAND_ALL_TYPES(F, _)
#undef F
	default:
		ASSERT(false && "invalid type");
		return 0;
	}
}

bool
type_is_native(TypeCode c)
{
	switch (c) {

#define F(tpe, _) case TypeCode_##tpe: return true;
		TYPE_EXPAND_NATIVE_TYPES(F, _)
#undef F
	default:
		return false;
	}
}

bool g_config_full_evaluation = false;
i64 g_config_vector_size = 0;

void print_string(const std::string& s, bool endl)
{
	std::cout << s;

	if (endl) {
		std::cout << std::endl;
	}
}

void
_debug_selection_vector_assert_order(sel_t* RESTRICT v, sel_t num,
	const char* func, const char* file, int line)
{
	ComplexFuncs::check_order(v, num, [&] (auto prev, auto curr) {{
		return prev < curr;
	}}, [&] (auto prev, auto val, auto idx) {{
		fprintf(stdout, "failing at %s i=%lld, prev=%lld, val=%lld\n", func, idx, prev, val);
		fflush(stdout);
		ASSERT(false);
	}});
}

sel_t
ComplexFuncs::selunion(sel_t* RESTRICT result,
	sel_t* RESTRICT larr, sel_t lnum,
	sel_t* RESTRICT rarr, sel_t rnum)
{
	static constexpr bool print = false;

	sel_t res[lnum+rnum];

	if (print) {
		printf("%s: res=%p larr=%p lnum=%lld rarr=%p rnum=%lld\n",
			__func__, res, larr, lnum, rarr, rnum);
	}

	ASSERT(larr && rarr);
	ASSERT(larr != rarr);

	debug_selection_vector_assert_order(larr, lnum);
	debug_selection_vector_assert_order(rarr, rnum);

	sel_t k=0, l=0, r=0;

	if (print) {
		for (int i=0; i<lnum; i++) {
			printf("left[%lld] = %lld\n", i, larr[i]);
		}
		for (int i=0; i<rnum; i++) {
			printf("right[%lld] = %lld\n", i, rarr[i]);
		}
	}

#if 0
	while (l < lnum && r < rnum) {
		printf("%s: compare %lld with %lld\n", __func__, larr[l], rarr[r]);
		if (larr[l] < rarr[r]) {
			if (print) {
				printf("%s: k=%lld from left %lld -> %lld\n", __func__, k, l, larr[l]);
			}
			res[k++] = larr[l++];
		} else {
			if (print) {
				printf("%s: k=%lld from right %lld -> %lld\n", __func__, k, r, rarr[r]);
			}
			res[k++] = rarr[r++];
			l += larr[l] == rarr[r];
		}
	}
#else
	while (l < lnum & r < rnum) {
		const auto lval = larr[l];
		const auto rval = rarr[r];

		bool le = lval <= rval;
		bool ge = lval >= rval;

		res[k] = lval;
		k += le;
		res[k] = rval;
		k += !le;

		l += le;
		r += ge;
	}
#endif

	while (l < lnum) {
		const auto val = larr[l];
		if (!k || val > res[k-1]) {
			if (print) {
				printf("%s: k=%lld fill left %lld -> %lld\n", __func__, k, l, larr[l]);
			}
			res[k++] = val;
		}

		l++;
	}

	while (r < rnum) {
		const auto val = rarr[r];
		if (!k || val > res[k-1]) {
			if (print) {
				printf("%s: k=%lld fill right %lld -> %lld\n", __func__, k, r, rarr[r]);
			}
			res[k++] = val;
		}

		r++;
	}
	if (print && res) {
		printf("union = %lld\n", k);
		for (int i=0; i<k; i++) {
			printf("union[%lld] = %lld\n", i, res[i]);
		}
	}

	debug_selection_vector_assert_order(res, k);

	// copy 'res' into 'result'
	for (size_t i=0; i<k; i++) {
		result[i] = res[i];
	}
	return k;
}

volatile void* __null_ptr = nullptr;

int
__dbg_test_prefetch(void* p)
{
	if (!p) return 0;
#ifdef IS_DEBUG
	auto w = (int64_t*)p;
#else
	auto w = (int*)__null_ptr;
#endif
	return *w;
}

#define EXPAND_TEMPORALITY(F, ARGS) \
	F(0, ARGS); \
	F(1, ARGS); \
	F(2, ARGS); \
	F(3, ARGS); 

template<typename T, typename U>
static void vec_pref_sample8(sel_t* sel, sel_t inum, const T& fA,
		bool optimistic, const U& fB, const char* msg = nullptr) {
	const auto old_num = inum;
	int k=0;

	optimistic &= Vectorized::optimistic_full_eval(sel, inum, 7, 8);
	if (optimistic) {
		auto start = sel[0];
		auto end = sel[inum-1];

		if (start > end) {
			std::swap(start, end);
		}

		// printf("optimistic\n");
		inum = sel[inum-1]+1;

		k = start;
		inum -= start;

		sel = nullptr;
	}

	auto start = rdtsc();
	if (sel) {
		for (; k+8<inum; k+=8) {
			fA(sel[k+0]);
			fA(sel[k+1]);
			fA(sel[k+2]);
			fA(sel[k+3]);
			fA(sel[k+4]);
			fA(sel[k+5]);
			fA(sel[k+6]);
			fA(sel[k+7]);

			fB(sel[k+0]);
			fB(sel[k+1]);
			fB(sel[k+2]);
			fB(sel[k+3]);
			fB(sel[k+4]);
			fB(sel[k+5]);
			fB(sel[k+6]);
			fB(sel[k+7]);
		}

		for (; k<inum; k++) {
			fA(sel[k+0]);
			
			fB(sel[k+0]);
		}
	} else {
		for (; k+8<inum; k+=8) {
			fA(k+0);
			fA(k+1);
			fA(k+2);
			fA(k+3);
			fA(k+4);
			fA(k+5);
			fA(k+6);
			fA(k+7);

			fB(k+0);
			fB(k+1);
			fB(k+2);
			fB(k+3);
			fB(k+4);
			fB(k+5);
			fB(k+6);
			fB(k+7);
		}

		for (; k<inum; k++) {
			fA(k+0);
			
			fB(k+0);
		}
	}

	auto end = rdtsc();

	if (false && msg) {
		printf("%s optimistic %d, cy/tup %f, inum %d\n",
			msg, optimistic,
			(double)(end-start) / (double)inum,
			old_num);
	}

}

const static bool disable_other_prefetch = true;


template<typename T>
void measure(const T& f, size_t num = 1, const char* prefix = nullptr) {
#ifdef PROFILE
	auto start = rdtsc();
	f();
	auto end = rdtsc();

	printf("%s: avg %.1f\n", prefix ? prefix : "measured", (double)(end-start) / (double)num);
#else
	f();
#endif
}

#ifdef __AVX512F__
#include <immintrin.h>
#include <emmintrin.h>
#include <xmmintrin.h>
#endif

void prefetch_bucket_lookup(sel_t* sel, sel_t num, int temporality,
	void** table, u64* hash, u64 mask)
{
	LOG_TRACE("%s: LWT %d: %s: sel=%p num=%d mask=%p\n",
		g_pipeline_name, g_lwt_id, __func__, sel, num, mask);

	debug_selection_vector_assert_order((sel_t*)sel, num);

#if 0
	int k = 0;

	auto start = rdtsc();

	auto vmask = _mm512_set1_epi64(mask);
	auto vtable = _mm512_set1_epi64((u64)table);

	u64 __attribute__((aligned(16))) buf[8];

	// num = 100;


	// TODO: use sel 
	for (; k+8<num; k+=8) {
		auto idx = _mm512_and_epi64(_mm512_loadu_si512(&hash[k]), vmask);

		static_assert(sizeof(table[0]) == 8, "log2(8) == 3");
		idx = _mm512_slli_epi64(idx, 3);
		idx = _mm512_add_epi32(idx, vtable);

		_mm512_store_si512(buf, idx);

#define B(pos) PREFETCH_DATA1(buf[pos]);
		B(0);
		B(1);
		B(2);
		B(3);
		B(4);
		B(5);
		B(6);
		B(7);
#undef B
	}

	auto end = rdtsc();

#if 0
	printf("%s cy/tup %d, inum %d\n",
		__func__, (end-start) / num,
		num);
#endif

#endif

#if 1
	measure([&] () {

	switch (temporality) {
#define A(TEMPORALITY, ARGS) \
	case TEMPORALITY: \
		vec_pref_sample8(sel, num, [&] (auto i) { \
			u64 idx = hash[i] & mask; \
			PREFETCH_DATA##TEMPORALITY(&table[idx]); \
		}, true, [&] (auto i) {}); \
		break;

EXPAND_TEMPORALITY(A, 0)

#undef A
	default:
		ASSERT(false && "invalid");
		break;
	}
#endif
	}, num, __func__);
}

void prefetch_bucket_next(sel_t* sel, sel_t num, int temporality,
	void** bucket, u64 next_offset)
{
	LOG_TRACE("%s: sel=%p num=%d offset=%p\n", __func__, sel, num, next_offset);
	debug_selection_vector_assert_order((sel_t*)sel, num);

	switch (temporality) {
#define A(TEMPORALITY, ARGS) \
	case TEMPORALITY: \
		vec_pref_sample8(sel, num, [&] (auto i) { \
			u64 next = *((u64*)((char*)bucket[i] + next_offset)); \
			PREFETCH_DATA##TEMPORALITY((void*)next); \
		}, false, [] (auto i) {}); \
		break;

EXPAND_TEMPORALITY(A, 0)

#undef A
	default:
		ASSERT(false && "invalid");
		break;
	}
}

void prefetch_bucket(sel_t* sel, sel_t num, int temporality,
	void** bucket, u64 offset)
{
	LOG_TRACE("%s: sel=%p num=%d offset=%p\n", __func__, sel, num, offset);
	debug_selection_vector_assert_order((sel_t*)sel, num);
	// ignore bucket

	switch (temporality) {
#define A(TEMPORALITY, ARGS) \
	case TEMPORALITY: \
		vec_pref_sample8(sel, num, [&] (auto i) { \
			PREFETCH_DATA##TEMPORALITY(bucket[i]); \
		}, false, [] (auto i) {}); \
		break;

EXPAND_TEMPORALITY(A, 0)

#undef A
	default:
		ASSERT(false && "invalid");
		break;
	}
}

sel_t avx512_seltrue(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred)
{
	sel_t k=0;
	sel_t i=0;

	debug_selection_vector_assert_order(sel, num);

	if (sel) {
#ifdef __AVX512F__
		const auto null8 = _mm512_set1_epi8(0);
		const auto mask = _mm512_set1_epi32(0xFF);

		for (;i+16<=num; i+=16) {
			auto sids = _mm512_loadu_si512(sel+i);

#if 0
			auto preds32 = _mm512_and_epi32(mask,
				_mm512_i32gather_epi32(sids, pred, 1));
			auto mask16 = _mm512_cmpeq_epi32_mask(preds32, null8);
#else
			// TODO: and+compare -> test
			auto mask16 = _mm512_test_epi32_mask(
				_mm512_i32gather_epi32(sids, pred, 1), mask);
#endif

			_mm512_mask_compressstoreu_epi32(res + k, mask16, sids);

			k += __builtin_popcount(mask16);
		}
#endif
		for (;i<num; i++) {
			if (pred[sel[i]]) {
				res[k] = sel[i];
				k++;
			}
		}
	} else {
#ifdef __AVX512F__
		auto ids = _mm512_set_epi32(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
		const auto null8 = _mm_set1_epi8(0);

		for (;i+16<=num; i+=16) {
			auto mask16 = _mm_cmpneq_epi8_mask(_mm_loadu_si128((__m128i*)(pred+i)), null8);

			_mm512_mask_compressstoreu_epi32(res + k, mask16, ids);

			k += __builtin_popcount(mask16);
			ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));
		}
#endif
		for (;i<num; i++) {
			if (pred[i]) {
				res[k] = i;
				k++;
			}
		}
	}

	LOG_TRACE("%s: LWT %d: %s(%p,%lld): returned %d\n", g_pipeline_name, g_lwt_id,  __func__, pred, num, (int)k);

	debug_selection_vector_assert_order((sel_t*)res, k);
	return k;
}


sel_t avx512_selfalse(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred)
{
	sel_t k=0;
	sel_t i=0;

	debug_selection_vector_assert_order(sel, num);

	if (sel) {
#ifdef __AVX512F__
		const auto null8 = _mm512_set1_epi8(0);
		const auto mask = _mm512_set1_epi32(0xFF);

		for (;i+16<=num; i+=16) {
			auto sids = _mm512_loadu_si512(sel+i);

#if 0
			auto preds32 = _mm512_and_epi32(mask,
				_mm512_i32gather_epi32(sids, pred, 1));
			auto mask16 = _mm512_cmpeq_epi32_mask(preds32, null8);
#else
			// TODO: and+compare -> test
			auto mask16 = _mm512_testn_epi32_mask(
				_mm512_i32gather_epi32(sids, pred, 1), mask);
#endif

			_mm512_mask_compressstoreu_epi32(res + k, mask16, sids);

			k += __builtin_popcount(mask16);
		}
#endif
		for (;i<num; i++) {
			if (!pred[sel[i]]) {
				res[k] = sel[i];
				k++;
			}
		}
	} else {
#ifdef __AVX512F__
		auto ids = _mm512_set_epi32(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
		const auto null8 = _mm_set1_epi8(0);

		for (;i+16<=num; i+=16) {
			auto mask16 = _mm_cmpeq_epi8_mask(_mm_loadu_si128((__m128i*)(pred+i)), null8);

			_mm512_mask_compressstoreu_epi32(res + k, mask16, ids);

			k += __builtin_popcount(mask16);
			ids = _mm512_add_epi32(ids, _mm512_set1_epi32(16));
		}
#endif
		for (;i<num; i++) {
			if (!pred[i]) {
				res[k] = i;
				k++;
			}
		}
	}

	LOG_TRACE("%s: LWT %d: %s(%p,%lld): returned %d\n", g_pipeline_name, g_lwt_id,  __func__, pred, num, (int)k);
	debug_selection_vector_assert_order((sel_t*)res, k);

	return k;
}

VEC_KERNEL_PRE sel_t vec_seltrue__u32__u8_col VEC_KERNEL_IN (sel_t* RESTRICT sel, sel_t inum, u32* RESTRICT res , u8* RESTRICT col1) VEC_KERNEL_POST;
VEC_KERNEL_PRE sel_t vec_selfalse__u32__u8_col VEC_KERNEL_IN (sel_t* RESTRICT sel, sel_t inum, u32* RESTRICT res , u8* RESTRICT col1) VEC_KERNEL_POST;

sel_t select_true(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred)
{
#ifdef __AVX512F__
	return avx512_seltrue(sel, num, res, pred);
#endif

	return vec_seltrue__u32__u8_col(sel, num, res, pred);
}
sel_t select_false(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred)
{
#ifdef __AVX512F__
	return avx512_selfalse(sel, num, res, pred);
#endif

	return vec_selfalse__u32__u8_col(sel, num, res, pred);
}



sel_t
avx512_bucket_lookup(sel_t* RESTRICT sel, sel_t num, u64* RESTRICT res,
	const u64* RESTRICT buckets, const u64* RESTRICT index, u64 mask)
{
	sel_t i=0;

	debug_selection_vector_assert_order(sel, num);

	if (sel) {
#ifdef __AVX512F__
		const auto vmask = _mm512_set1_epi64(mask);

		for (;i+16<num; i+=16) {
			__m256i sids_a = _mm256_loadu_si256((__m256i*)(sel+i));
			__m256i sids_b = _mm256_loadu_si256((__m256i*)(sel+i+8));

			auto idx64_a =_mm512_i32gather_epi64(sids_a, index, 8);
			auto idx64_b =_mm512_i32gather_epi64(sids_b, index, 8);

			idx64_a = _mm512_and_epi64(idx64_a, vmask);
			idx64_b = _mm512_and_epi64(idx64_b, vmask);
			
			idx64_a = _mm512_i64gather_epi64(idx64_a, buckets, 8);
			idx64_b = _mm512_i64gather_epi64(idx64_b, buckets, 8);

			_mm512_i32scatter_epi64(res, sids_a, idx64_a, 8);
			_mm512_i32scatter_epi64(res, sids_b, idx64_b, 8);
		}
#endif

		for (;i<num; i++) {
			auto k = sel[i];
			res[k] = buckets[index[k] & mask];
		}
	} else {
		for (;i<num; i++) {
			auto k = i;
			res[k] = buckets[index[k] & mask];
		}
	}

	return num;
}

sel_t
avx512_bucket_next(sel_t* RESTRICT sel, sel_t num, u64* RESTRICT res,
	u64 *RESTRICT bucket, u64 next_offset)
{
	sel_t i=0;

	debug_selection_vector_assert_order(sel, num);

	if (sel) {
#ifdef __AVX512F__
		for (;i+16<num; i+=16) {
			__m256i sids_a = _mm256_loadu_si256((__m256i*)(sel+i));
			__m256i sids_b = _mm256_loadu_si256((__m256i*)(sel+i+8));

			auto idx64_a =_mm512_i32gather_epi64(sids_a, bucket, 8);
			auto idx64_b =_mm512_i32gather_epi64(sids_b, bucket, 8);
			
			idx64_a = _mm512_i64gather_epi64(idx64_a, (void*)next_offset, 1);
			idx64_b = _mm512_i64gather_epi64(idx64_b, (void*)next_offset, 1);

			_mm512_i32scatter_epi64(res, sids_a, idx64_a, 8);
			_mm512_i32scatter_epi64(res, sids_b, idx64_b, 8);
		}
#endif

		for (;i<num; i++) {
			auto k = sel[i];
			u64* RESTRICT data = (u64* RESTRICT)((char* RESTRICT)bucket[k] + next_offset);
			res[k] = *data;
		}
	} else {
		for (;i<num; i++) {
			auto k = i;
			u64* RESTRICT data = (u64* RESTRICT)((char* RESTRICT)bucket[k] + next_offset);
			res[k] = *data;
		}
	}

	return num;
}

sel_t
avx512_check__u8__u32_col_u64_col_u64_col_u32_col(sel_t* RESTRICT sel, sel_t num,
	u8* RESTRICT res, u64* RESTRICT data, u32* RESTRICT key, u64 offset)
{
	sel_t i=0;

	debug_selection_vector_assert_order(sel, num);

	if (sel) {
#ifdef __AVX512F__
		for (;i+16<num; i+=16) {
#define A(msk, p, off) { res[sel[i + p + off]] = msk & (1 << p); }
			auto sids_a = _mm256_loadu_si256((__m256i*)(sel+i));
			auto idx64_a =_mm512_i32gather_epi64(sids_a, data, 8);
			auto key_a =_mm256_i32gather_epi32((const int*)key, sids_a, 4);
			auto val_a = _mm512_i64gather_epi32(idx64_a, (void*)offset, 1);
			auto msk_a = _mm256_cmpeq_epi32_mask(val_a, key_a);
		
			auto sids_b = _mm256_loadu_si256((__m256i*)(sel+i+8));
			auto idx64_b =_mm512_i32gather_epi64(sids_b, data, 8);
			auto key_b =_mm256_i32gather_epi32((const int*)key, sids_b, 4);
			auto val_b = _mm512_i64gather_epi32(idx64_b, (void*)offset, 1);
			auto msk_b = _mm256_cmpeq_epi32_mask(val_b, key_b);

			A(msk_a, 0, 0);
			A(msk_a, 1, 0);
			A(msk_a, 2, 0);
			A(msk_a, 3, 0);
			A(msk_a, 4, 0);
			A(msk_a, 5, 0);
			A(msk_a, 6, 0);
			A(msk_a, 7, 0);
		
			A(msk_b, 0, 8);
			A(msk_b, 1, 8);
			A(msk_b, 2, 8);
			A(msk_b, 3, 8);
			A(msk_b, 4, 8);
			A(msk_b, 5, 8);
			A(msk_b, 6, 8);
			A(msk_b, 7, 8);
		
#undef A
		}
#endif

		for (;i<num; i++) {
			auto k = sel[i];
			u32* RESTRICT _data = (u32* RESTRICT)((char* RESTRICT)data[k] + offset);
			res[k] = *_data == key[k];
		}
	} else {
		for (;i<num; i++) {
			auto k = i;
			u32* RESTRICT _data = (u32* RESTRICT)((char* RESTRICT)data[k] + offset);
			res[k] = *_data == key[k];
		}
	}

	return num;
}

sel_t
avx512_vec_eq__u8__u64_col_u64_col(sel_t* RESTRICT sel, sel_t inum,
	u8* RESTRICT res, u64* RESTRICT a, u64* RESTRICT b)
{
	int i=0;
	if (Vectorized::optimistic_full_eval(sel, inum)) {
		inum = sel[inum-1]+1;
		sel = nullptr;
	}

	if (sel) {
		for (;i<inum; i++) {
			auto k= sel[i];
			res[k] = a[k] == b[k];
		}
	} else {
#ifdef __AVX512F__
		const u64 mask = (1l << 0) | (1l << 8) | (1l << 16) | (1l << 24) | (1l << 32) | (1l << 40) | (1l << 48) | (1l << 56);
		u64* RESTRICT res8 = (u64*)res;
		for (;i+16<inum; i+=16) {
			auto a1 = _mm512_loadu_si512((__m512i*)(a+i));
			auto b1 = _mm512_loadu_si512((__m512i*)(b+i));
			auto m1 = _mm512_cmpeq_epi64_mask(a1, b1);

			auto a2 = _mm512_loadu_si512((__m512i*)(a+i+8));
			auto b2 = _mm512_loadu_si512((__m512i*)(b+i+8));
			auto m2 = _mm512_cmpeq_epi64_mask(a2, b2);
			// bit -> byte
			res8[i/8] = _pdep_u64(m1, mask);
			res8[i/8 + 1] = _pdep_u64(m2, mask);
		}
#endif
		for (;i<inum; i++) {
			res[i] = a[i] == b[i];
		}
	}

	return inum;
}

#include "runtime_simd.hpp"

sel_t
avx512_hash_u32(sel_t* RESTRICT sel, sel_t inum,
	u64* RESTRICT res, u32* RESTRICT a)
{
	int i=0;
	if (Vectorized::optimistic_full_eval(sel, inum)) {
		inum = sel[inum-1]+1;
		sel = nullptr;
	}

	if (sel) {
		for (;i<inum; i++) {
			auto k = sel[i];
			res[k] = voila_hash<u32>::hash(a[k]);
		}
	} else {
#ifdef __AVX512F__
		for (; i+16<inum; i+=16) {
			auto h1 = avx512_voila_hash(_mm256_loadu_si256((__m256i*)(a+i)));
			auto h2 = avx512_voila_hash(_mm256_loadu_si256((__m256i*)(a+i+8)));
			_mm512_storeu_si512(res+i, h1);
			_mm512_storeu_si512(res+i+8, h2);
		}
#endif

		for (;i<inum; i++) {
			auto k = i;
			res[k] = voila_hash<u32>::hash(a[k]);
		}
	}

	return inum;
}

sel_t
Vectorized::predicate_buf_write_typed(pred_t* RESTRICT array,
		sel_t* RESTRICT data, size_t offset, sel_t* sel, sel_t num,
		sel_t sel_num, const char* dbg_col_var, int dbg_line)
{
	LOG_TRACE("%s LWT %d@%d: WRITE_PRED(%p): sel=%p data=%p num=%d with=%d into %s\n",
		g_pipeline_name, g_lwt_id, dbg_line, array, sel, data, num, sel_num, dbg_col_var);

	debug_selection_vector_assert_order(data, sel_num);

	if (data) {
		// num = 128; // FIXME: replace with vector size
	} else {

	}
	ASSERT(!sel);

	sel_t max;
	ASSERT(num >= sel_num);

	pred_t * RESTRICT out = &array[offset];
	if (data) {
		if (sel_num > 0) {
			max = data[sel_num-1]+1;
		} else {
			max = 0;
		}

		Vectorized::map(sel, num, [&] (int i) {
			ASSERT(i < 1024);
			out[i] = false;
			LOG_TRACE("%s: write_pred(%p): @@%d false at i=%d SEL. %p\n",
				g_pipeline_name, array, offset+i, i, &out[i]);
		}, true);

		Vectorized::map(sel, sel_num, [&] (auto i) {
			// LOG_DEBUG("i=%d data=%d\n", i, data[i]);
			LOG_TRACE("%s: write_pred(%p): @@%d true at i=%d SEL. %p\n",
				g_pipeline_name, array, offset+data[i], data[i], &out[data[i]]);
			// DBG_ASSERT(data[i] >= 0 && data[i] <= num);
			out[data[i]] = true;

			ASSERT(data[i] <= max);

			ASSERT(data[i] < 1024);
			ASSERT(data[i] < num);
		}, false);
	} else {
		max = num;
		Vectorized::map(sel, num, [&] (int i) {
			out[i] = true;
			LOG_TRACE("%s: write_pred(%p): @@%d true at i=%d NOSEL. %p\n",
				g_pipeline_name, array, offset+i, i, &out[i]);
			// LOG_DEBUG("%s: false at i=%d\n", __func__, i);
		}, true);
	}


#if 0
	// TEST REMOVEME:
	sel_t t1[1024];
	sel_t t0;
	t0 = predicate_buf_read_typed(t1, array, offset, nullptr, max, true);

	if (data) {
		ASSERT(t0 == sel_num);

		for (sel_t i=0; i<t0; i++) {
			ASSERT(t1[i] == data[i]);
		}
	} else {
		ASSERT(t0 == num);
	}
#endif
	return sel_num;
}

const char* g_pipeline_name = "";

// int g_lwt_id = 0;

#ifdef TRACE 
const char* g_buffer_name = "";
int g_check_line = 0;
#endif



#ifdef __AVX__
// from https://helloacm.com/the-rdtsc-performance-timer-written-in-c/
//  Windows
#ifdef _WIN32
 
#include <intrin.h>
uint64_t rdtsc() {
    return __rdtsc();
}
 
//  Linux/GCC
#else
uint64_t rdtsc(){
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}
 
#endif
#else
uint64_t rdtsc() {
  return 0;
}

#endif
