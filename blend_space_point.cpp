#include "blend_space_point.hpp"
#include <iostream>
#include "runtime.hpp"
#include "utils.hpp"

static int blend_config_parse_bool(const str& t) {
	if (t == "1") {
		return true;
	} else if (t == "0") {
		return false;
	} else {
		ASSERT(false);
		return false;
	}
}

static int blend_config_parse_int(const str& t) { return std::stoi(t); }
static str blend_config_parse_str(const str& t) { return t; }

bool
BlendConfig::is_null() const {
	return computation_type.empty();
}

const static std::string kVector = "vec";
const static std::string kScalar = "scalar";
const static std::string kAvx = "avx512";

bool
BlendConfig::is_vectorized() const {
	if (is_null()) {
		return false;
	}

	return startsWith(computation_type, kVector);
}

void
BlendConfig::from_string(const std::string& parse_string)
{
	// initialize
#define INIT(type, name, dflt, args) \
	name = dflt;

	EXPAND_BLEND_CONFIG(INIT, _)

	// parse short cuts
	if (!parse_string.compare("") || !parse_string.compare("NULL") ||
			!parse_string.compare("null")) {
		computation_type = "";
		return;
	} else if (!parse_string.compare("hyper")) {
		computation_type = kScalar;
		return;
	} else if (!parse_string.compare("x100")) {
		computation_type = kVector;
		return;
	}

#define PARSE(type, name, __default, args) \
	if (!key.compare(#name)) {\
		name = blend_config_parse_##type(val); \
		return true; \
	}

	const auto unparsable = ConfigParser::parse(parse_string,
		[&] (const str& key, const str& val) -> bool {
			EXPAND_BLEND_CONFIG(PARSE, _)
			std::cerr << "Invalid option '" << key<< "'" << std::endl;
			ASSERT(false && "invalid option");
			return false;
		}
	);

	ASSERT(unparsable.empty());

#undef INIT
#undef PARSE

	ASSERT(prefetch >= 0 && prefetch <= 4);

	if (!computation_type.empty()) {
		ASSERT(str_in_strings(computation_type, {kScalar, kAvx})
			|| startsWith(computation_type, kVector));
	}
}

std::string
BlendConfig::to_string() const
{
	std::ostringstream r;
	bool first = true;

	if (is_null()) {
		return "NULL";
	}

#define TO_STR(type, name, __default, args) \
	if (!first) { \
		r << ","; \
	} \
	first = false; \
	r << #name << "=" << name;

EXPAND_BLEND_CONFIG(TO_STR, _)
#undef TO_STR

	return r.str();
}

bool
BlendConfig::operator==(const BlendConfig& other) const
{
#define CHECK(type, name, dflt, args) if (name != other.name) return false;
	EXPAND_BLEND_CONFIG(CHECK, _)
#undef CHECK
	return true;
}



#include "explorer_helper.hpp"

BlendSpacePoint::BlendSpacePoint() {
	default_flavor = (*generate_blends(kGenBlendOnlyBase))[0];
}

std::string
BlendSpacePoint::to_string() const
{
	std::ostringstream o;

	o << "{" << std::endl;

	o << "  \"" << "default" << "\" : \"" << default_flavor->to_string() << "\"";

	for (size_t pid=0; pid<pipelines.size(); pid++) {
		const auto& pipeline = pipelines[pid];
		if (pipeline.ignore) {
			continue;
		}
		o << "," << std::endl;

		o << "  \"" << pid << "\" : {" << std::endl;
		o << "    \"flavor\" : \"" << pipeline.flavor->to_string() << "\"";

		for (size_t ins=0; ins<pipeline.point_flavors.size(); ins++) {
			const auto& pos = pipeline.point_flavors[ins];
			o << "," << std::endl;
			o << "    \"" << ins << "\" : \"" << pos->to_string() <<"\"";
		}
		o << std::endl << "  }";
	}
	o << std::endl << "}" << std::endl;

	return o.str();
}

bool
BlendSpacePoint::is_valid() const
{
	return true;
}