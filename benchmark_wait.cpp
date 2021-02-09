#include "benchmark_wait.hpp"
#include "popen.hpp"
#include "runtime.hpp"
#include "utils.hpp"

#include <chrono>
#include <thread>

void
BenchmarkWait::operator()() const
{
	auto should_wait1 = [&] () -> int {
		POpenRead_t read;

		std::cerr << read.open("ps aux");

		std::stringstream result;
		read.spinCaptureAll(result);

		const auto str = result.str();

		auto r = read.close();
		if (!r.empty()) {
			std::cerr << "POpenRead_t returned error: " << r << std::endl;
			return -1;
		}

		// some process blacklisted
		for (auto& b : blacklist) {
			if (str.find(b) != std::string::npos) {
				std::cerr << "process: " << b << std::endl;
				return -1;
			}
		}

		// filter the ones with cpu > 0.1
		auto lines = split(str, '\n');

		for (auto& line : lines) {
			// ignore GDB
			bool ignore = false;
			for (auto& w : whitelist) {
				if (str.find(w) != std::string::npos) {
					ignore = true;
					break;
				}
			}

			if (ignore) {
				continue;
			}

			// detect CPU time
			auto cols = split(line, ' ');
			int col_id = 0;
			for (auto& col : cols) {
				if (col.empty()) {
					continue;
				}
				col_id++;

				if (col_id == 3) {
					double cpu;
					try {
						cpu = std::stod(col);
					} catch (std::invalid_argument a) {
						cpu = 0.0;
					}

					if (cpu > 25.0) {
						std::cerr << "line: "<< line << std::endl;
						std::cerr << "cpu = " << cpu << std::endl;
						return -2;
					}

				}
			}

		}
	  	
	  	return 0;
	};

	auto should_wait = [&] () -> int {
		int r = 0;
		for (int i=0; i<4; i++) {
			r = should_wait1();
			if (r) {
				return r;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return r;
	};

	while (1) {
		const int w = should_wait();
		if (!w) {
			std::cerr << "Machine is clear, proceeding ..." << std::endl;
			break;
		}

		switch (w) {
		case -1:
			std::cerr << "Detected running blacklisted process ..." << std::endl;
			break;

		case -2:
			std::cerr << "Detected process(es) consuming significant CPU time ..." << std::endl;
			break;
		default:
			ASSERT(false); break;
		} 

		std::cerr << "Waiting 1s ..." << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
}