#include "utils.hpp"
#include <fstream>

void
FileUtils::write_string_to_file(const std::string& path, const std::string& data)
{
	std::ofstream f;
	f.open(path);
	f << data;
	f.close();
}

void
FileUtils::append_string_to_file(const std::string& path, const std::string& data)
{
	std::ofstream f;
	f.open(path, std::ios_base::app);
	f << data;
	f.close();
}

std::string
FileUtils::read_string_from_file(const std::string& path)
{
	std::ifstream f(path);
	std::stringstream s;
	s << f.rdbuf();
	return s.str();
}

bool
FileUtils::exists(const std::string& file)
{
	std::ifstream f(file);
	return f.good();
}

#include <functional>

static void
_uppercase(std::string& str)
{
	std::transform(str.begin(), str.end(),str.begin(), ::toupper);
}

std::string
uppercase(const std::string& str)
{
	std::string r(str);
	_uppercase(r);	
	return r;
}

std::string
replace_all(std::string str, const std::string& from, const std::string& to)
{
	// from https://stackoverflow.com/questions/2896600/how-to-replace-all-occurrences-of-a-character-in-string
	size_t start_pos = 0;
	while((start_pos = str.find(from, start_pos)) != std::string::npos) {
		str.replace(start_pos, from.length(), to);
		start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
	}
	return str;
}