#ifndef H_UTILS
#define H_UTILS

#include "runtime.hpp"

#include <vector>
#include <sstream>
#include <iostream>
/*
std::string split implementation by using delimiter as a character.
from https://thispointer.com/how-to-split-a-string-in-c/
*/
inline static std::vector<std::string> split(std::string strToSplit, char delimeter)
{
	std::stringstream ss(strToSplit);
	std::string item;
	std::vector<std::string> splittedStrings;
	while (std::getline(ss, item, delimeter))
	{
	   splittedStrings.push_back(item);
	}
	return splittedStrings;
}

inline static bool
parse_cardinal(long long& res, const std::string& v)
{
	try {
		res = stoll(v);
		return true;
	} catch (...) {
		return false;
	}
}

inline static bool
str_in_strings(const std::string& s, const std::vector<std::string>& ts)
{
	for (auto& t : ts) {
		if (!s.compare(t)) {
			return true;
		}
	}
	return false;
}

/* Generic matcher
 *
 * Matcher::match1(
 *     [] () {return false;},
 *     [] () {return true; ... Everything else ... },
 * );
 */
struct Matcher {
	template<typename T>
	static bool
	match1(T v)
	{
		return v();
	}

	template<typename T, typename... Args>
	static bool
	match1(T first, Args... args)
	{
		if (first()) {
			return true;
		}
		return match1(args...);
	}
};

#include <unordered_set>
template<typename RESULT>
struct SetResolver {
	std::unordered_set<RESULT> set;

	template<typename T, typename RESOLVE>
	void add(const T& t, RESOLVE&& resolver) {
		set.insert(resolver(t));
	}
};

inline static std::string clear_stream(std::ostringstream& s) {
	std::string r = s.str();
	s.clear();
	s.str("");
	return r;
}


#include <algorithm>

struct Functional {
	template<typename R, typename S, typename T>
	static R fold(const S& ts, const R& first, const T& fun)
	{
		R res = first;

		for (const auto& t : ts) {
			res = fun(res, t);
		}

		return res;
	}

	template<typename R, typename S, typename T>
	static R join(const S& ps, const R& sep, const T& map_fun) {
		bool first = true;
		R result;
		for (const auto& p : ps) {
			if (!first) {
				result = result + sep;
			}
			result = result + map_fun(p);
			first = false;
		}
		return result;
	}

	template<typename R, typename S>
	static R join(const S& ps, const R& sep) {
		return join(ps, sep, [&] (auto i) { return i; });
	}

	template<typename T>
	static void sort_dupl_inplace(T& result)
	{
		std::sort(result.begin(), result.end());
		result.erase(std::unique(result.begin(), result.end()), result.end());
	}

	template<typename S, typename T>
	static bool contains(const S& v, const T& x)
	{
		return std::find(v.begin(), v.end(), x) != v.end();
	}
};

struct FileUtils {
	static void write_string_to_file(const std::string& path, const std::string& data);
	static void append_string_to_file(const std::string& path, const std::string& data);
	static std::string read_string_from_file(const std::string& path);
	static bool exists(const std::string& file);
};

struct ConfigParser {
public:
	static constexpr char kFieldDelim = ',';
	static constexpr char kValueDelim = '=';

	template<typename T>
	static std::string parse(const std::string& parse_string, const T& on_item) {
		std::vector<std::string> options = split(parse_string, kFieldDelim);
		std::string unparsable;

		for (auto& option : options) {
			std::vector<std::string> fields = split(option, kValueDelim);
			ASSERT(fields.size() == 2);

			const auto& key = fields[0];
			const auto& val = fields[1];

			if (!on_item(key, val)) {
				// invalid
				unparsable = unparsable.empty() ? option : unparsable + kFieldDelim + option;
				continue;
			}
		}

		return unparsable;
	}

};

#include <functional>

std::string uppercase(const std::string& str);

std::string replace_all(std::string str, const std::string& from,
	const std::string& to);

template <class T>
bool startsWith(const T &s, const T &t)
{
	// from https://stackoverflow.com/a/7913920
	return s.size() >= t.size() &&
		std::equal( t.begin(), t.end(), s.begin() );
}

template<typename T>
void first_n_inplace(T& list, size_t n)
{
	T result;

	size_t num = std::min(n, list.size());
	result.reserve(num);

	for (size_t i=0; i<num; i++) {
		result.emplace_back(list[i]);
	}

	list = std::move(result);
}
#endif