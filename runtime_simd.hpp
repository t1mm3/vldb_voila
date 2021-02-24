#ifndef H_RUNTIME_SIMD
#define H_RUNTIME_SIMD

#ifdef __AVX512F__
#include <immintrin.h>
#include "runtime.hpp"

#define DECL_TYPE_OPTS(N, BITS) N _##N[BITS / (8*sizeof(N))];

union _v64 {
	TYPE_EXPAND_NATIVE_TYPES(DECL_TYPE_OPTS, 64)

	__m64 _dv;
	__m64 _iv;
	__m64 v;
};

union _v128 {
	TYPE_EXPAND_NATIVE_TYPES(DECL_TYPE_OPTS, 128)

	__m128d _dv;
	__m128i _iv;
	__m128 v;
};

union _v256 {
	TYPE_EXPAND_NATIVE_TYPES(DECL_TYPE_OPTS, 256)

	__m256d _dv;
	__m256i _iv;
	__m256 v;
};


union _v512 {
	TYPE_EXPAND_NATIVE_TYPES(DECL_TYPE_OPTS, 512)

	__m512d _dv;
	__m512i _iv;
	__m512 v;
};
#undef DECL_TYPE_OPTS

template<typename T, size_t N>
struct _fbuf {
	T a[N];

	static constexpr size_t kNum = N;
};

template<>
struct _fbuf<bool, 8> {
	bool a[8];

	static constexpr size_t kNum = 8;

	operator __mmask8() {
#define A(i) if (a[i]) { r = r | (1 << i); }
		__mmask8 r = 0;
		A(0);A(1);A(2);A(3);
		A(4);A(5);A(6);A(7);
		return r;
#undef A
	}

	_fbuf(__mmask8 m) {
#define A(i) { a[i] = m & (1 << i); }
		A(0);A(1);A(2);A(3);
		A(4);A(5);A(6);A(7);
#undef A	
	}

	_fbuf() {
	}
};

template<typename T, size_t N>inline constexpr _fbuf<T, N>
_fbuf_set1(const T& n)
{
	_fbuf<T, N> r;
	for (size_t i=0; i<_fbuf<T, N>::kNum; i++) {
		r.a[i] = n;
	}
	return r;
}

template<typename T, size_t N>inline constexpr void
_fbuf_set1(_fbuf<T, N>& r, const T& n)
{
	r = _fbuf_set1<T, N>(n);
}

inline constexpr void
_v512_from(_v512& out, const __m512& in) { out.v = in; }
inline constexpr void
_v512_from(_v512& out, const __m512i& in) { out._iv = in; }

template<typename T>
inline constexpr _v512 _v512_from(const T& x) {
	_v512 r;
	_v512_from(r, x);
	return r;
}

inline constexpr void
_v256_from(_v256& out, const __m256& in) { out.v = in; }
inline constexpr void
_v256_from(_v256& out, const __m256i& in) { out._iv = in; }

template<typename T>
inline constexpr _v256 _v256_from(const T& x) {
	_v256 r;
	_v256_from(r, x);
	return r;
}

inline constexpr void
_v128_from(_v128& out, const __m128& in) { out.v = in; }
inline constexpr void
_v128_from(_v128& out, const __m128i& in) { out._iv = in; }

template<typename T>
inline constexpr _v128 _v128_from(const T& x) {
	_v128 r;
	_v128_from(r, x);
	return r;
}


inline constexpr void
_v64_from(_v64& out, const __m64& in) { out.v = in; }

template<typename T>
inline constexpr _v64 _v64_from(const T& x) {
	_v64 r;
	_v64_from(r, x);
	return r;
}


inline constexpr __mmask8 _mask8_from_num(size_t num) {
	u32 r = (1ull << num) -1;
	// LOG_TRACE("%s: num=%lld mask=%lld\n", __func__, num, r);
	return r;
}

#define EXPAND_SIMD_PRINT(F, A) \
	F(_v512, _v512_print, A) \
	F(_v256, _v256_print, A) \
	F(_v128, _v128_print, A) \
	F(_v64, _v64_print, A)


#define DECL(vec, func, A) \
	void func(const vec& a, const TypeCode& type, const char* prefix = nullptr, const char* suffix = nullptr); \
	inline void func(const vec& a, const char* type, const char* prefix = nullptr, const char* suffix = nullptr) { \
		func(a, type_code_from_str(type), prefix, suffix); \
	} \
	inline void print_vec(const vec& a, const char* type, const char* prefix = nullptr, const char* suffix = nullptr) { \
		func(a, type, prefix, suffix); \
	}

EXPAND_SIMD_PRINT(DECL, 0)

#undef EXPAND_SIMD_PRINT

template<typename T>
void print_fbuf(const T& f, const char* prefix) {
	if (prefix) {
		printf(prefix);
	}

	for (size_t i=0; i<f.kNum; i++) {
		if (i) {
			printf(", ");
		}
		printf("%lld", f.a[i]);
	}
	printf("\n");
}


inline char popcount32(u32 a) {
	auto r = _mm_popcnt_u32(a);
	// printf("%s(%lld)=%lld\n", __func__, a, r);
	return r;
}
#if 0
inline char popcount64(u64 a) {
	return _mm_popcnt_u64(a);
}
#endif



// Macro wrapper for easy use with clite

#define SIMD_GET_IVEC(x) (x)._iv

#define SIMD_FBUF_GET(x, i) (x).a[i]
#define SIMD_REG_GET(X, TYPE, INDEX) (X)._##TYPE[INDEX]


#define SIMD_TABLE_COLUMN_OFFSET(T, COL) (T)->offset_##COL  
#define SIMD_TABLE_NEXT_OFFSET(T) (T)->next_offset


// Emulate some gather, to avoid SIMD -> scalar overhead

inline __m128i _mm512_i64gather_epi16(__m512i index, void *const addr, int scale) {
	return _mm256_cvtepi32_epi16(_mm512_i64gather_epi32(index, addr, scale));
}

inline __m128i _mm512_mask_i64gather_epi16(__m128i src, __mmask8 mask, __m512i index,
		void *const addr, int scale) {
	__m256i null32 = _mm256_set1_epi32(0);
	__m256i gather32 = _mm512_mask_i64gather_epi32(null32, mask, index, addr, scale);
	return _mm256_mask_cvtepi32_epi16(src, mask, gather32);
}

inline __m512i avx512_voila_hash(__m512i x) {
#if 1
	const auto c = _mm512_set1_epi64(0x45d9f3b);

	x = _mm512_xor_epi64(x, _mm512_srai_epi64(x, 16));
	x = _mm512_mullo_epi64(x, c);

	x = _mm512_xor_epi64(x, _mm512_srai_epi64(x, 16));
	x = _mm512_mullo_epi64(x, c);

	x = _mm512_xor_epi64(x, _mm512_srai_epi64(x, 16));
#else
	const uint64_t m = 0xc6a4a7935bd1e995;
	const int r = 47;
	u64 seed = 0;
	auto h = _mm512_set1_epi64(seed ^ 0x8445d61a4e774912 ^ (8 * m));

	const auto c = _mm512_set1_epi64(m);

	auto k = x;
	// k *= m;
	// k ^= k >> r;
	k = _mm512_mullo_epi64(k, c);
	k = _mm512_xor_epi64(k, _mm512_srai_epi64(k, r));

	// k *= m;
	k = _mm512_mullo_epi64(k, c);


	// h ^= k;
	h = _mm512_xor_epi64(h, k);


	// h *= m;
	h = _mm512_mullo_epi64(h, c);

/*
-               h ^= h >> r; \
-               h *= m; \
-               h ^= h >> r; \
-               return h; \
*/
	h = _mm512_xor_epi64(h, _mm512_srai_epi64(h, r));
	h = _mm512_mullo_epi64(h, c);

	h = _mm512_xor_epi64(h, _mm512_srai_epi64(h, r));
	return h;


#endif
	return x;
}

inline __m512i avx512_voila_hash(__m256i x) {
	return avx512_voila_hash(_mm512_cvtepi32_epi64(x));
}

inline __m512i avx512_voila_hash(__m128i x) {
	return avx512_voila_hash(_mm512_cvtepi16_epi64(x));
}

template<typename T>
inline __m512i avx512_voila_hash(T x, __m512i h) {
	
	return _mm512_xor_epi64(avx512_voila_hash(x),
		_mm512_mullo_epi64(h, _mm512_set1_epi64(__voila_hash::kConst)));
}


template<typename T>
static void avx512_compressed8_epi64(__mmask8 mask, __m512i data, const T& f)
{
	if (mask == 0xFF) {
		auto& seq = data;
		f(seq, 7);
		f(seq, 6);
		f(seq, 5);
		f(seq, 4);
		f(seq, 3);
		f(seq, 2);
		f(seq, 1);
		f(seq, 0);
		return;
	}
	auto num = popcount32(mask);
	auto seq = _mm512_maskz_compress_epi64(mask, data);

	switch (num) {
	case 8: f(seq, 7);
	case 7: f(seq, 6);
	case 6: f(seq, 5);
	case 5: f(seq, 4);
	case 4: f(seq, 3);
	case 3: f(seq, 2);
	case 2: f(seq, 1);
	case 1: f(seq, 0);
	case 0: break;
	};
}


template<int64_t C>
static __m512i avx512_mulconst_epi64(__m512i data, int64_t _c)
{
	switch (C) {
	case 1: return data;
	case 2:	return _mm512_slli_epi64(data, 1);
	case 4:	return _mm512_slli_epi64(data, 2);
	case 8:	return _mm512_slli_epi64(data, 3);
	case 16:	return _mm512_slli_epi64(data, 4);
	case 32:	return _mm512_slli_epi64(data, 5);
	case 64:	return _mm512_slli_epi64(data, 6);
	case 128:	return _mm512_slli_epi64(data, 7);
	case 256:	return _mm512_slli_epi64(data, 8);
	case 512:	return _mm512_slli_epi64(data, 9);
	case 1024:	return _mm512_slli_epi64(data, 10);
	case 2048:	return _mm512_slli_epi64(data, 11);
	default:
		return _mm512_mullo_epi64(data, _mm512_set1_epi64(_c));
	}
}

#endif
#endif
