#ifndef H_BLEND_CONTEXT
#define H_BLEND_CONTEXT

#include <string>
#include <unordered_map>
#include <memory>

#include "clite.hpp"
#include "voila.hpp"
#include "cg_fuji_data.hpp"

struct FlowGenerator;
struct DataGen;
struct FujiCodegen;
struct State;

struct BlendContext {
	const std::string c_name;
	std::unique_ptr<DataGen> data_gen;
	BlendConfig config;

	DataGenBufferPtr input_buffer;
	DataGenBufferPtr output_buffer;

	BlendContext(FujiCodegen& codegen, const BlendConfig* const _config,
		BlendContext* prev_blend);

	bool operator==(const BlendContext& other) const;
	bool operator!=(const BlendContext& other) const {
		return !(*this == other);
	}

	clite::Block* entry_state;
	clite::Block* current_state;

	void add_state(clite::Block* s);
};

enum BaseFlavor {
	Unknown = 0,
	Avx512,
	Scalar,
	Vector,
};

BaseFlavor parse_computation_type(size_t& o_unroll, const QueryConfig& config,
	const std::string& computation_type);

#endif