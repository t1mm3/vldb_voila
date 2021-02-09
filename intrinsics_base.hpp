#ifndef H_INTRINSICS_BASE
#define H_INTRINSICS_BASE

#include <string>
#include <vector>

struct IntrinsicTableInstallEntry {
	std::vector<const char*> cpuid;
	const char* name;
	const char* tech;
	const char* result_type;
	const char* category;

	struct Arg {
		const char* type;
		const char* name;
	};

	const std::vector<Arg> args;
};

#endif