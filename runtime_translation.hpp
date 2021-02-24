#ifndef H_RUNTIME_TRANSLATION
#define H_RUNTIME_TRANSLATION

#include "runtime.hpp"
#include "runtime_simd.hpp"

#ifdef __AVX512F__
template<typename T, int N, typename F>
inline void __set_fbuf(_fbuf<T, N>& p, const F& fun)
{
	for (int i=0; i<N; i++) {
		p.a[i] = fun(i);
	}
}

template<typename MASK_TYPE, int N, typename T>
inline void
__translate_pred_avx512_from_scalar(MASK_TYPE& out, const _fbuf<T, N>& p)
{
	out = 0;

	for (int i=0; i<N; i++) {
		out |= (p.a[i] & 1) << i; 
	}

#if 0
	print_fbuf(p, "translate_pred_avx512_from_scalar: ");
	printf("translate_pred_avx512_from_scalar: res %d\n", out);
#endif
}

template<typename MASK_TYPE, int N>
inline void
__translate_pred_scalar_from_avx512(_fbuf<bool, N>& out, const MASK_TYPE& p)
{
	__set_fbuf<bool, N>(out, [] (int i) { return false; });

	for (int i=0; i<N; i++) {
		out.a[i] = (1 << i) & p;
	}

#if 0
	printf("translate_pred_scalar_from_avx512: input %d\n", p);
	print_fbuf(out, "translate_pred_scalar_from_avx512: ");
#endif
}


inline void
translate_pred_avx512__from_scalar(__mmask8& out, const _fbuf<pred_t, 8>& p)
{
	__translate_pred_avx512_from_scalar<__mmask8, 8, pred_t>(out, p);
}

inline void
translate_pred_avx512__from_scalar(__mmask8& out, const _fbuf<u8, 8>& p)
{
	__translate_pred_avx512_from_scalar<__mmask8, 8, u8>(out, p);
}

inline void
translate_pred_avx512__to_scalar(_fbuf<bool, 8>& out, const __mmask8& p)
{
	__translate_pred_scalar_from_avx512<__mmask8, 8>(out, p);
}


#if 0
inline void
translate_pred_avx512_from_scalar(_fbuf<bool, 8>& out, const __mmask8& p)
{
	__translate_pred_scalar_from_p<__mmask8, 8>(out, p);
}
#endif
#endif

inline sel_t
translate_pred_scalar_to_vec(sel_t* RESTRICT out, const bool* RESTRICT p, sel_t num)
{
	return select_true(nullptr, num, (u32*)out, (u8*)p);
}

inline sel_t
translate_pred_vec_to_scalar(bool* RESTRICT out, const sel_t* RESTRICT p, sel_t num)
{
	Vectorized::map(nullptr, g_config_vector_size, [&] (int i) {
			out[i] = false;
	}, true);

	Vectorized::map(nullptr, num, [&] (auto i) {
		out[p[i]] = true;
	}, false);

	return g_config_vector_size;
}


#endif
