#ifndef H_BLEND_SPACE_POINT
#define H_BLEND_SPACE_POINT

#include <string>
#include <vector>
#include <memory>

typedef std::string str;

struct BlendConfig {
#define EXPAND_BLEND_CONFIG(A, ARGS) \
	A(int, concurrent_fsms, 2, ARGS) \
	A(str, computation_type, "scalar", ARGS) \
	A(int, prefetch, 0, ARGS)


#define DECL(type, name, dflt, _) type name;
	EXPAND_BLEND_CONFIG(DECL, 0)
#undef DECL

	std::string to_string() const;
	bool is_null() const;

	bool is_vectorized() const;

	bool operator==(const BlendConfig& other) const;
	bool operator!=(const BlendConfig& other) const {
		return !(*this == other);
	}

	size_t hash_comp() const {
		return std::hash<str>()(computation_type);
	}

	size_t hash() const {
		return ((std::hash<int>()(concurrent_fsms)
				^ (hash_comp() << 1)) >> 1)
				^ (std::hash<int>()(prefetch) << 1);
	}

	void from_string(const std::string& parse_string);
	BlendConfig(const std::string& parse_string = "") {
		from_string(parse_string);
	}
};


namespace std {

	template <>
	struct hash<BlendConfig>
	{
		std::size_t operator()(const BlendConfig& k) const
		{
			using std::size_t;
			using std::hash;
			using std::string;

			return k.hash();
		}
	};

}

typedef std::shared_ptr<BlendConfig> BlendConfigPtr;


struct BlendSpacePoint {
	struct Pipeline {
		bool ignore = false;
		BlendConfig* flavor;

		std::vector<BlendConfig*> point_flavors;

		bool operator==(const Pipeline& other) const {
			return ignore == other.ignore && flavor == other.flavor
				&& point_flavors == other.point_flavors;
		}
	};

	std::vector<Pipeline> pipelines; //!< Per pipeline flavors

	BlendConfig* default_flavor = nullptr;

	bool operator==(const BlendSpacePoint& other) const {
		return other.pipelines == pipelines;
	}

	std::string to_string() const;
	bool is_valid() const;

	BlendSpacePoint();
};

namespace std {
	template <>
	struct hash<BlendSpacePoint::Pipeline>
	{
		std::size_t operator()(const BlendSpacePoint::Pipeline& k) const
		{
			using std::size_t;
			using std::hash;
			using std::string;

			std::size_t seed = ((hash<bool>()(k.ignore)
				^ (hash<BlendConfig>()(*k.flavor) << 1)) >> 1);


			for (auto& p : k.point_flavors) {
				seed ^= hash<BlendConfig>()(*p) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
			}
			return seed;
		}
	};
	template <>
	struct hash<BlendSpacePoint>
	{
		std::size_t operator()(const BlendSpacePoint& k) const
		{
			using std::size_t;
			using std::hash;
			using std::string;

			std::size_t seed = k.pipelines.size();
			for (auto& p : k.pipelines) {
				seed ^= hash<BlendSpacePoint::Pipeline>()(p) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
			}
			return seed;
		}
	};

}

#endif