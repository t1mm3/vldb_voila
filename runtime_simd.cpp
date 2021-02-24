#include "runtime_simd.hpp"
#ifdef __AVX512F__
#define PRINT_TYPE(N, BITS) case TypeCode_##N: num = BITS / (8*sizeof(N));\
	for (size_t i=0; i<num; i++) { \
		printf("%s%lld", i == 0 ? "" : ", ", (long long int)a._##N[i]); \
	} \
	break;

void
_v512_print(const _v512& a, const TypeCode& type, const char* prefix, const char* postfix)
{
	size_t num;

	if (prefix) {
		printf("%s", prefix);
	}

	switch (type) {
	TYPE_EXPAND_NATIVE_TYPES(PRINT_TYPE, 512)

	default:
		ASSERT(false);
		break;
	}

	if (postfix) {
		printf("%s", postfix);
	}
	printf("\n");
}

void
_v256_print(const _v256& a, const TypeCode& type, const char* prefix, const char* postfix)
{
	size_t num;

	if (prefix) {
		printf("%s", prefix);
	}

	switch (type) {
	TYPE_EXPAND_NATIVE_TYPES(PRINT_TYPE, 256)

	default:
		ASSERT(false);
		break;
	}

	if (postfix) {
		printf("%s", postfix);
	}
	printf("\n");
}

void
_v128_print(const _v128& a, const TypeCode& type, const char* prefix, const char* postfix)
{
	size_t num;

	if (prefix) {
		printf("%s", prefix);
	}

	switch (type) {
	TYPE_EXPAND_NATIVE_TYPES(PRINT_TYPE, 128)

	default:
		ASSERT(false);
		break;
	}

	if (postfix) {
		printf("%s", postfix);
	}
	printf("\n");
}


void
_v64_print(const _v64& a, const TypeCode& type, const char* prefix, const char* postfix)
{
	size_t num;

	if (prefix) {
		printf("%s", prefix);
	}

	switch (type) {
	TYPE_EXPAND_NATIVE_TYPES(PRINT_TYPE, 64)

	default:
		ASSERT(false);
		break;
	}

	if (postfix) {
		printf("%s", postfix);
	}
	printf("\n");
}
#endif
