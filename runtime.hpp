#ifndef H_RUNTIME
#define H_RUNTIME

#include <stddef.h>
#include <vector>
#include <unordered_map>
#include <string>
#include <stdexcept>
#include <cstring>
#include <cassert>

#ifdef __AVX2__
#include <xmmintrin.h>
#endif

#define PERFORMANCE_MODE

// #define PROFILE

// #define TRACE

#ifdef IS_RELEASE
	#ifdef IS_DEBUG
	#error Cannot be both Debug and Release build
	#endif
#endif

#ifndef IS_RELEASE
	#ifndef IS_DEBUG
	#error Has to be either Debug or Release build
	#endif
#endif


#ifdef IS_RELEASE
// #define PERFORMANCE_MODE
#endif



#if 1
#ifdef PERFORMANCE_MODE
#define DISABLE_ASSERTS
#endif
#endif


#ifndef DISABLE_ASSERTS
#define ASSERT(x) if (UNLIKELY(!(x))) { assert(x); /* throw std::runtime_error(std::string("Assertion failed:") + std::string(#x)); */ }
#else
#define ASSERT(x)
#endif


#define __LOG(...) /* fprintf(stdout, __VA_ARGS__); fflush(stdout) */

extern const char* g_pipeline_name;

#ifdef TRACE 
extern const char* g_buffer_name;
extern int g_check_line;
#endif

#ifndef PERFORMANCE_MODE
#define LOG_TRACE(...) /* printf(__VA_ARGS__); */
#define LOG_SCHED_TRACE(...)  /* printf(__VA_ARGS__); */
#define LOG_DEBUG(...) __LOG(__VA_ARGS__);
#else
#define LOG_TRACE(...) /* printf(__VA_ARGS__); */
#define LOG_SCHED_TRACE(...) /* printf(__VA_ARGS__); */
#define LOG_DEBUG(...) /* printf(__VA_ARGS__); */
#endif


#define LOG_ERROR(...) fprintf(stdout, __VA_ARGS__); fflush(stdout)

#define LOG_GEN_TRACE(...) /* printf(__VA_ARGS__) */


#ifdef IS_DEBUG
#define DBG_ASSERT(x) ASSERT(x)
#else
#define DBG_ASSERT(x) (void)(x)
#endif

#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define LIKELY(expr) __builtin_expect(!!(expr), 1)

#define RESTRICT __restrict
#define EXPORT extern "C"
#define NOVECTORIZE __attribute__((optimize("no-tree-vectorize")))

#define NOINLINE __attribute__ ((noinline))

enum TypeClass {
	Cardinal,
	String
};

#define CARDINAL_TYPE_EXPAND(f, arg) \
	f(u8, unsigned char, TypeClass::Cardinal, arg) \
	f(i8, signed char, TypeClass::Cardinal, arg) \
	f(i16, int16_t, TypeClass::Cardinal, arg) \
	f(u16, uint16_t, TypeClass::Cardinal, arg) \
	f(i32, int32_t, TypeClass::Cardinal, arg) \
	f(u32, uint32_t, TypeClass::Cardinal, arg) \
	f(i64, int64_t, TypeClass::Cardinal, arg) \
	f(u64, uint64_t, TypeClass::Cardinal, arg) \
	f(i128, __int128, TypeClass::Cardinal, arg)

#define u8 unsigned char
#define i8 signed char

#define i16 int16_t
#define u16 uint16_t

#define i32 int32_t
#define u32 uint32_t

#define i64 int64_t
#define u64 uint64_t

void print_string(const std::string& s, bool endl = true);

template<typename S, typename T>
bool contains(const S& s, const T& t)
{
	return s.contains(t);
}

inline void splitJulianDay(unsigned jd, unsigned& year, unsigned& month, unsigned& day)
	// Algorithm from the Calendar FAQ
{
   unsigned a = jd + 32044;
   unsigned b = (4*a+3)/146097;
   unsigned c = a-((146097*b)/4);
   unsigned d = (4*c+3)/1461;
   unsigned e = c-((1461*d)/4);
   unsigned m = (5*e+2)/153;

   day = e - ((153*m+2)/5) + 1;
   month = m + 3 - (12*(m/10));
   year = (100*b) + d - 4800 + (m/10);
}

inline u32 extract_year(u32 date)
{
   unsigned year,month,day;
   splitJulianDay(date,year,month,day);
   return year;
}

struct varchar {
	uint16_t len;
	const char* arr = nullptr;

	bool operator==(const varchar& a) const {
		bool r = a.len == len && !strncmp(a.arr, arr, len);
#if 0
		print_string("compare: '", false);
		print_string(as_string(), false);
		print_string("' with '", false);
		print_string(a.as_string(), false);
		print_string("'", true);

		if (r) {
			print_string("yes");
		} else {
			print_string("no");
		}
#endif
		return r;
	}

	bool operator!=(const varchar& a) const {
		return !(a == *this);
	}

	varchar() {
		// produce segfault!
		len = 3;
		arr = "XXX";
	}

	varchar(const char* s) {
		arr = s;
		len = strlen(s);
	}
#if 0
	varchar(const std::string& s) {
		arr = s.c_str();
		len = s.size();
	}
#endif
	varchar(void* data, size_t idx, size_t max_len) {
		const char* val = (const char*)data;
		varchar r;

#define LENGTH_IND uint8_t

		size_t sz = sizeof(LENGTH_IND) + max_len;
		const char* dest = &val[idx * sz];

		len = *((LENGTH_IND*)dest);
		arr = dest + sizeof(LENGTH_IND);
#undef LENGTH_IND
	}

	bool contains(const varchar& v) const {
		return memmem(arr, len, v.arr, v.len);
	}

	std::string as_string() const {
		return std::string(arr, len);
	}

	void debug_print() const {
		print_string(as_string());
	}

	varchar& operator=(const char* msg) {
		arr = msg;
		len = strlen(msg);

		return *this;
	}
};

#define hash_t u64

#ifdef __GNUC__
#define i128 __int128_t
#define u128 __uint128_t
#else
#error "i128 not implemented"
#endif

#define sel_t i32
#define pred_t bool
#define pos_t i64

int __dbg_test_prefetch(void* p);

#if 0
#define DBG_TEST_PREFETCH(p) __dbg_test_prefetch((void*)p)
#else
#define DBG_TEST_PREFETCH(p)
#endif

#ifdef __GNUC__
#define __PREFETCH(ptr, rw, tmp) DBG_TEST_PREFETCH(ptr); __builtin_prefetch((const void*)ptr, rw, tmp)
#else
#error "Compiler doesn't support prefetch"
#endif

#ifdef __AVX__
#define PREFETCH_DATA0(ptr) _mm_prefetch((const void*)ptr, _MM_HINT_T0)
#define PREFETCH_DATA1(ptr) _mm_prefetch((const void*)ptr, _MM_HINT_T1)
#define PREFETCH_DATA2(ptr) _mm_prefetch((const void*)ptr, _MM_HINT_T2)
#define PREFETCH_DATA3(ptr) _mm_prefetch((const void*)ptr, _MM_HINT_NTA)
#else
#define PREFETCH_DATA0(ptr) __PREFETCH((const void*)ptr, 0, 1)
#define PREFETCH_DATA1(ptr) __PREFETCH((const void*)ptr, 0, 2)
#define PREFETCH_DATA2(ptr) __PREFETCH((const void*)ptr, 0, 3)
#define PREFETCH_DATA3(ptr) __PREFETCH((const void*)ptr, 1, 3)
#endif

#define TYPE_EXPAND_NATIVE_TYPES(F, a) \
	F(u8, a) \
	F(i8, a) \
	F(u16, a) \
	F(i16, a) \
	F(u32, a) \
	F(i32, a) \
	F(u64, a) \
	F(i64, a)


#define TYPE_EXPAND_CARDINAL_TYPES(F, a) \
	TYPE_EXPAND_NATIVE_TYPES(F, a) \
	F(i128, a)


#define TYPE_EXPAND_ALL_TYPES(F, a) \
	TYPE_EXPAND_NATIVE_TYPES(F, a) \
	F(i128, a) \
	F(varchar, a)

typedef enum TypeCode {
	TypeCode_invalid=0,

#define F(t, _) TypeCode_##t, 
	TYPE_EXPAND_ALL_TYPES(F, _)
#undef F
} TypeCode;

TypeCode type_code_from_str(const char* str);
inline TypeCode 
type_code_from_str(const std::string& s)
{
	return type_code_from_str(s.c_str());
}
size_t type_width_bytes(TypeCode c);
bool type_is_native(TypeCode c);

inline size_t
type_width_bits(TypeCode c)
{
	return 8*type_width_bytes(c);
}


template <typename T>
struct voila_cast {
	
};


#define TYPE_EXPAND_VOILA_CAST(F, a) \
	F(u8, u8, false, a) \
	F(i8, u8, true, a) \
	F(u16, u16, false, a) \
	F(i16, u16, true, a) \
	F(u32, u32, false, a) \
	F(i32, u32, true, a) \
	F(u64, u64, false, a) \
	F(i64, i64, true, a) \
	F(i128, u128, true, a)

template <>
struct voila_cast <varchar> {
	static size_t to_cstr(char*& obuf, char* buffer, size_t buffersz, varchar& value) {
		(void)buffer;
		(void)buffersz;
		obuf = (char*)value.arr;
		return value.len;
	}

	#define A(TYPE, _) static TYPE to_##TYPE(const varchar& value) { return std::stoll(std::string(value.arr, value.len)); }
	TYPE_EXPAND_CARDINAL_TYPES(A, 0)
	#undef A

	static varchar to_varchar(varchar& value) { return value; }


	static void print(const varchar& value) {
		print_string(value.as_string());
	}

	static constexpr size_t good_buffer_size() {
		return 0;
	}
};

#define F_A(TYPE, SOURCE) static TYPE to_##TYPE(SOURCE& value) { return value; }

#define F(TYPE, UTYPE, CAST_TO_U, _) \
template <> \
struct voila_cast <TYPE> { \
	static size_t to_cstr(char*& obuf, char* buffer, size_t buffersz, const TYPE& value) { \
		/* from https://stackoverflow.com/questions/25114597/how-to-print-int128-in-g */ \
		UTYPE tmp; \
		if (CAST_TO_U) tmp = value < 0 ? -value : value; \
		else tmp = value; \
		char* d = buffer + buffersz-1; \
		*d = 0; \
		do { \
			DBG_ASSERT((size_t)d > (size_t)buffer); \
			-- d; \
		    *d = "0123456789"[tmp % 10]; \
		    tmp /= 10; \
		} while (tmp != 0); \
		\
		if (value < 0) { \
			DBG_ASSERT((size_t)d > (size_t)buffer); \
		    d--; \
		    *d = '-'; \
		} \
		obuf = d; \
		return (buffer+buffersz) - d - 1; \
	} \
	\
	TYPE_EXPAND_CARDINAL_TYPES(F_A, TYPE) \
	static varchar to_varchar(TYPE& value) { (void)value; return varchar(); } \
	\
	static constexpr size_t good_buffer_size() { return 41; /* 128-bit integer + sign + 0 */ } \
	\
	static void print(const TYPE& value) { \
		static const size_t bufsz = good_buffer_size(); \
		char buf[bufsz]; \
		char* str = nullptr; \
		size_t sz = to_cstr(str, buf, bufsz, value); \
		printf("%s", str); \
		print_string(std::string(str, sz)); \
	} \
};

TYPE_EXPAND_VOILA_CAST(F, _)
#undef F
#undef F_A


#include <iostream>
template <typename T>
struct print_helper {
	static T print(T& v) {
		std::cout << voila_cast<T>::to_string(v);
		return v;
	}
};


struct __voila_hash {
	static constexpr u64 kConst = 2654435761;
};

template <typename T>
struct voila_hash {

};

#define F(TYPE, UTYPE, CAST_TO_U, _) \
template <> \
struct voila_hash <TYPE> { \
	static u64 hash(const TYPE& v) { \
		u64 x = (u64)v; \
		x ^= x >> 16; \
		x *= UINT64_C(0x45d9f3b); \
		x ^= x >> 16; \
		x *= UINT64_C(0x45d9f3b); \
		x ^= x >> 16; \
		return x; \
	} \
	\
	static u64 rehash(const u64 h, const TYPE& v) { \
		return hash(v) ^ (h * __voila_hash::kConst); \
	} \
};

TYPE_EXPAND_VOILA_CAST(F, _)
#undef F

template <>
struct voila_hash <varchar> {
	static u64 hash(varchar v, u64 seed = 0) {
		const char* key = v.arr;
		const size_t len = v.len;

		// MurmurHash64A
		// MurmurHash2, 64-bit versions, by Austin Appleby
		// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;

		uint64_t h = seed ^ (len * m);

		const uint64_t* data = (const uint64_t*)key;
		const uint64_t* end = data + (len / 8);

		while (data != end) {
			uint64_t k = *data++;

			k *= m;
			k ^= k >> r;
			k *= m;

			h ^= k;
			h *= m;
		}

		const unsigned char* data2 = (const unsigned char*)data;

		switch (len & 7) {
		case 7: h ^= uint64_t(data2[6]) << 48;
		case 6: h ^= uint64_t(data2[5]) << 40;
		case 5: h ^= uint64_t(data2[4]) << 32;
		case 4: h ^= uint64_t(data2[3]) << 24;
		case 3: h ^= uint64_t(data2[2]) << 16;
		case 2: h ^= uint64_t(data2[1]) << 8;
		case 1: h ^= uint64_t(data2[0]); h *= m;
		};

		h ^= h >> r;
		h *= m;
		h ^= h >> r;

		return h;
	}
	static u64 rehash(const u64 h, varchar v) {
		return hash(v, h);
	}
};

#define VEC_KERNEL_POST
#define VEC_KERNEL_PRE extern "C"
#define VEC_KERNEL_IN

#define VEC_KERNEL_PROLOGUE(sel, num) LOG_TRACE("%s: LWT %d: kernel %s res=%p sel=%p num=%d\n", g_pipeline_name, g_lwt_id, __func__, res, sel, num);
#define VEC_KERNEL_EPILOGUE(sel, num)


#ifdef IS_DEBUG
#define debug_selection_vector_assert_order(v, n)  _debug_selection_vector_assert_order(v, n, __func__, __FILE__, __LINE__)
#else
#define debug_selection_vector_assert_order(v, n) 
#endif
void _debug_selection_vector_assert_order(sel_t* RESTRICT v, sel_t num, const char* func, const char* file, int line);


struct ComplexFuncs {
	template<typename R, typename C, typename F>
	static bool check_order(R* RESTRICT v, sel_t num, const C& compare, const F& fail)
	{
		LOG_TRACE("%s; check order %p num %d\n", __func__, v, (int)num);
		if (!v || !num) {
			return true;
		}
		R prev = v[0];

		for (sel_t i=1; i<num; i++) {
			if (!compare(prev, v[i])) {
				fail(prev, v[i], i);
				return false;
			}

			prev = v[i];
		}

		return true;
	}

	static sel_t selunion(sel_t* RESTRICT res, sel_t* RESTRICT larr, sel_t lnum, sel_t* RESTRICT rarr, sel_t rnum);
};

#define CLITE_PRINT(x) __clite_print(x, #x, __FILE__, __LINE__)

template<typename T>
T __clite_print(const T& x, const std::string& msg, const std::string& file, int line)
{
	std::cout << "print: '" << msg << "' = '" << x << "' @" << file << ":" << line << std::endl;
	return x;
}


void prefetch_bucket_lookup(sel_t* sel, sel_t num, int temporality, void** table, u64* hash, u64 mask);
void prefetch_bucket_next(sel_t* sel, sel_t num, int temporality, void** bucket, u64 offset);
void prefetch_bucket(sel_t* sel, sel_t num, int temporality, void** bucket, u64 offset);

sel_t avx512_seltrue(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred);
sel_t avx512_selfalse(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred);

sel_t select_true(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred);
sel_t select_false(sel_t* RESTRICT sel, sel_t num, u32* RESTRICT res, u8* RESTRICT pred);


sel_t avx512_bucket_lookup(sel_t* RESTRICT sel, sel_t num, u64* RESTRICT res, const u64* RESTRICT buckets, const u64* RESTRICT index, u64 mask);
sel_t avx512_bucket_next(sel_t* RESTRICT sel, sel_t num, u64* RESTRICT res, u64 *RESTRICT bucket, u64 next_offset);
sel_t
avx512_check__u8__u32_col_u64_col_u64_col_u32_col(sel_t* RESTRICT sel, sel_t inum, u8* RESTRICT res,
	u64* RESTRICT data, u32* RESTRICT key, u64 offset);

sel_t
avx512_vec_eq__u8__u64_col_u64_col(sel_t* RESTRICT sel, sel_t inum,
	u8* RESTRICT res, u64* RESTRICT a, u64* RESTRICT b);


sel_t
avx512_hash_u32(sel_t* RESTRICT sel, sel_t inum,
	u64* RESTRICT res, u32* RESTRICT a);


extern bool g_config_full_evaluation;
extern i64 g_config_vector_size;

#if 0
extern int g_lwt_id;
#else
#define g_lwt_id 0
#endif

struct Vectorized {
	static bool optimistic_full_eval(sel_t* sel, sel_t inum, size_t mul = 1, size_t div = 2) {
		if (inum > 0 && sel && g_config_full_evaluation) {
#if 0
			if (g_config_vector_size / 2 > inum) {
				return true;
			}
#else
			size_t start_idx = sel[0];
			size_t end_idx = sel[inum-1]+1;

			if (end_idx < start_idx) {
				std::swap(end_idx, start_idx);
			}

			return ((mul*end_idx) / div) <= inum;
#endif
		}
		return false;
	}

	template<typename T>
	static void map(sel_t* sel, sel_t inum, const T& fun, bool optimistic) {
		int k=0;
		if (optimistic && optimistic_full_eval(sel, inum)) {
			inum = sel[inum-1]+1;
			sel = nullptr;
		}

		if (sel) {
			for (; k+8<inum; k+=8) {
				fun(sel[k+0]);
				fun(sel[k+1]);
				fun(sel[k+2]);
				fun(sel[k+3]);
				fun(sel[k+4]);
				fun(sel[k+5]);
				fun(sel[k+6]);
				fun(sel[k+7]);
			}
			for (; k<inum; k++) {
				fun(sel[k]);
			}
		} else {
			for (; k<inum; k++) {
				fun(k);
			}
		}
	}

	template<typename T>
	static void map8(sel_t* sel, sel_t inum, const T& fun, bool optimistic) {
		int k=0;
		if (optimistic && optimistic_full_eval(sel, inum)) {
			inum = sel[inum-1]+1;
			sel = nullptr;
		}

		if (sel) {
			for (; k+8<inum; k+=8) {
				fun(sel[k+0]);
				fun(sel[k+1]);
				fun(sel[k+2]);
				fun(sel[k+3]);
				fun(sel[k+4]);
				fun(sel[k+5]);
				fun(sel[k+6]);
				fun(sel[k+7]);
			}
			for (; k<inum; k++) {
				fun(sel[k]);
			}
		} else {
			for (; k+8<inum; k+=8) {
				fun(k+0);
				fun(k+1);
				fun(k+2);
				fun(k+3);
				fun(k+4);
				fun(k+5);
				fun(k+6);
				fun(k+7);
			}
			for (; k<inum; k++) {
				fun(k);
			}
		}
	}

	template<typename T>
	static void NOINLINE buf_read_typed(T* RESTRICT result, T* RESTRICT array, size_t offset,
			sel_t* sel, sel_t num) {
		sel_t k=0;
		ASSERT(!sel);
		Vectorized::map(sel, num, [&] (auto i) {
			result[k] = array[offset+k];
			k++;
		}, false);
	}

	template<typename T>
	static void buf_read(void* array, void* data, size_t offset,
			sel_t* sel, sel_t num) {
		buf_read_typed<T>((T*)array, (T*)data, offset, sel, num);
	}

	static sel_t NOINLINE predicate_buf_read_typed(sel_t* RESTRICT result, pred_t* RESTRICT array, size_t offset,
			sel_t* sel, sel_t num, bool silent = false) {

		if (!silent) {
			LOG_TRACE("%s: res=%p, array=%p, offset=%p\n",
				__func__, result, array, offset);

			LOG_DEBUG("READ_PRED(%p): sel=%p num=%d\n", array, sel, num);
		}

		// #error should be the same
		auto k = select_true(sel, num, (u32*)result, (u8*)&array[offset]);

		LOG_TRACE("%s: LWT %d: %s: -> sel=%p, onum=%d\n",
			g_pipeline_name, g_lwt_id, __func__, result, k);

		return k;
	}

	static sel_t predicate_buf_read(void* RESTRICT result, void* RESTRICT array, size_t offset,
			sel_t* sel, sel_t num, const char* dbg_col_var, int dbg_line) {
		LOG_TRACE("%s: LWT %d@%d: %s: res=%p, array=%p, offset=%p from %s\n",
			g_pipeline_name, g_lwt_id, dbg_line, __func__, result, array, offset, dbg_col_var);
		return predicate_buf_read_typed((sel_t*)result, (bool*)array, offset, sel, num);
	}

	template<typename T>
	static void NOINLINE buf_write_typed(T* RESTRICT array, T* RESTRICT data, size_t offset,
			sel_t* sel, sel_t num) {
		ASSERT(!sel);
		sel_t k=0;
		Vectorized::map(sel, num, [&] (auto i) {
			array[offset+k] = data[i];
			k++;
		}, false);
	}

	template<typename T>
	static void buf_write(void* array, void* data, size_t offset,
			sel_t* sel, sel_t num) {
		LOG_TRACE("%s: array=%p, data=%p, offset=%p\n",
			__func__, array, data, offset);
		buf_write_typed<T>((T*)array, (T*)data, offset, sel, num);
	}

	static sel_t NOINLINE predicate_buf_write_typed(pred_t* RESTRICT array,
			sel_t* RESTRICT data, size_t offset, sel_t* sel, sel_t num,
			sel_t sel_num, const char* dbg_col_var, int dbg_line);

	static sel_t predicate_buf_write(void* RESTRICT array, void* RESTRICT data,
			size_t offset, sel_t* sel, sel_t num, sel_t sel_num, const char* dbg_col_var,
			int dbg_line) {
		return predicate_buf_write_typed((bool*)array, // + offset,
			(sel_t*)data, offset, sel, num, sel_num,
			dbg_col_var, dbg_line);
	}
};


uint64_t rdtsc();

static const u64 kGlobalAggrHashVal=42;

#endif 
