#include "common/runtime/Types.hpp"
#include <string>
#include <iostream>
#include <fstream>

int main() {
	std::string line;
    while(std::getline(std::cin, line)) {
    	types::Date d = types::Date::castString(line);

    	printf("%d\n", d.value);
    }
	return 0;
}