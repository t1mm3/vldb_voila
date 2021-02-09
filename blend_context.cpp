#include "blend_context.hpp"
#include "runtime.hpp"
#include "cg_fuji.hpp"
#include "cg_fuji_control.hpp"
#include "cg_fuji_avx512.hpp"
#include "cg_fuji_scalar.hpp"
#include "cg_fuji_vector.hpp"
#include "utils.hpp"
#include "runtime_framework.hpp"

BaseFlavor parse_computation_type(size_t& o_unroll, const QueryConfig& config,
	const std::string& ct)
{
	o_unroll = 1;
	if (!ct.compare("avx") || !ct.compare("avx512")) {
		return BaseFlavor::Avx512;
	} else if (!ct.compare("scalar")) {
		return BaseFlavor::Scalar;
	} else {
		// assuming vectorized
		o_unroll = 0;
		std::vector<std::string> options = split(ct, '(');

		if (options.empty()) {
			std::cerr << "Cannot parse computation_type '" << ct << "'" << std::endl;
			return BaseFlavor::Unknown;
		}
		ASSERT(options.size() > 0);

		auto& name = options[0];
		ASSERT(name == "vector" || name == "x100");

		if (options.size() > 1) {
			ASSERT(options.size() == 2);
			o_unroll = std::stoi(split(options[1], ')')[0]);

			ASSERT(o_unroll > 0);
		} else {
			o_unroll = config.vector_size;

			ASSERT(o_unroll > 0);
		}

		return BaseFlavor::Vector;
	}


	return BaseFlavor::Unknown;
}

BlendContext::BlendContext(FujiCodegen& codegen, const BlendConfig* const _config,
	BlendContext* previous_blend)
{
	if (_config) {
		config = *_config;
	} else {
		BlendConfig default_config;
		config = default_config;
	}

	data_gen = nullptr;

	size_t osize = 0;
	auto ct = parse_computation_type(osize, codegen.config,
		config.computation_type);

	switch (ct) {
	case BaseFlavor::Avx512:
		data_gen = std::make_unique<Avx512DataGen>(codegen);
		break;

	case BaseFlavor::Scalar:
		data_gen = std::make_unique<ScalarDataGen>(codegen);
		break;

	case BaseFlavor::Vector:
		data_gen = std::make_unique<VectorDataGen>(codegen, osize);
		break;

	default:
		std::cerr << "Invalid flavor for config=" << config.to_string()
			<< std::endl;
		ASSERT(false);
		exit(1);
		break;
	}

	entry_state = nullptr;
	current_state = nullptr;
}

bool
BlendContext::operator==(const BlendContext& other) const {
	return config == other.config;
}

void
BlendContext::add_state(clite::Block* s)
{
	if (!entry_state) {
		entry_state = s;
	}
	current_state = s;

	LOG_DEBUG("%p: add_state '%s'\n", this, s->dbg_name.c_str());
}