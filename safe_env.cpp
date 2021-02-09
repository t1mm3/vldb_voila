#include "safe_env.hpp"

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "runtime.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <iostream>
#include <thread>
#include <future>

SafeEnv::SafeEnv(const std::string& sharename, int thread_id, bool safe,
	int timeout_secs)
 : m_safe_env(safe), m_share_name(sharename), m_timeout(timeout_secs),
 m_thread_id(thread_id)
{
	if (!safe) {
		// ASSERT(timeout_secs <= 0);
	}
}

static const int kSuccess = 42;


struct Share {
	int fd;
	int* mem;
	size_t mem_size;

	std::string name;
};

struct ShareManager {
	std::unordered_map<int, Share> share_files;

	~ShareManager() {
		for (auto& shares_kv : share_files) {
			auto& share = shares_kv.second;

			if (share.mem != MAP_FAILED) {
				int r = munmap(share.mem, share.mem_size);
				if (r) {
					std::cerr << "munmap() failed with " << r << " "
						<< "'" << strerror(errno) << "'" << std::endl;
				}
			}

#if 0
			int r = shm_unlink(share.name.c_str());
			if (r) {
				std::cerr << "shm_unlink() failed with " << r << " "
					<< "'" << strerror(errno) << "'" << std::endl;
			}
#endif
		}
	}
};

static std::mutex g_mutex;
static ShareManager g_shares;

SafeEnv::Result
SafeEnv::operator()(const std::function<void()>& safe)
{
	// no safety required
	if (!m_safe_env) {
		safe();
		std::cerr << "Child exited successfully" << std::endl;

		return Result::Success;
	}

	const size_t msize = getpagesize();
	int* shared_memory;

	{
		std::lock_guard<std::mutex> guard(g_mutex);

		auto it = g_shares.share_files.find(m_thread_id);
		if (it == g_shares.share_files.end()) {
			shm_unlink(m_share_name.c_str());

			int shm_fd = shm_open(m_share_name.c_str(), O_CREAT | O_EXCL | O_RDWR,
				S_IRWXU | S_IRWXG);
			if (shm_fd < 0) {
				std::cerr << "Error in shm_open(): " << strerror(errno) << std::endl;
				ASSERT(false);
				return Result::Error;
			}

			ftruncate(shm_fd, msize);

			// allocating the shared memory
			shared_memory = (int *) mmap(NULL, msize, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
			if (shared_memory == MAP_FAILED) {
				std::cerr << "Error in mmap()" << std::endl;
				return Result::Error;
			}


			g_shares.share_files.insert({m_thread_id,
				{ shm_fd, shared_memory, msize, m_share_name } });
		} else {
			shared_memory = it->second.mem;
		}

		shared_memory[0] = 0;
	}

	auto wrapped = [&] () {
		

		int pid = fork();

		if (!pid) {
			// child
			safe();

			shared_memory[0] = kSuccess;

			// terminate child successfully
			std::cerr << "Child exited successfully" << std::endl;
			exit(0);
		} else if (pid > 0) {
			// master -> pid of child

			// wait until child exited
			int status;

			if (m_timeout > 0) {
				auto future = std::async(std::launch::async, &wait, &status);
				if (future.wait_for(std::chrono::seconds(m_timeout)) == std::future_status::timeout) {
					kill(pid, SIGKILL);
					return Result::Timeout;
				}
			} else {
				int child_pid = wait(&status);
				(void)child_pid;
			}

			if (shared_memory[0] == kSuccess) {
				std::cerr << "Child exited with status successfully" << std::endl;
				return Result::Success;
			}

			if (WIFEXITED(status)) {
				std::cerr << "Child exited with status " << WEXITSTATUS(status) << std::endl;

				if (WEXITSTATUS(status) == 0) {
					return Result::Success;
				} else {
					std::cerr << "Child failed" << std::endl;
					return Result::Crash;
				}
			}

			return Result::Crash;
		} else {
			std::cerr << "Cannot fork " << pid << std::endl;
			ASSERT(false);
			return Result::Error;
		}
	};

	auto result = wrapped();

#if 0
	if (shared_memory != MAP_FAILED) {
		int r = munmap(shared_memory, msize);
		if (r) {
			std::cerr << "munmap() failed with " << r << " "
				<< "'" << strerror(errno) << "'" << std::endl;
		}
	}

	int r = shm_unlink(m_share_name.c_str());
	if (r) {
		std::cerr << "shm_unlink() failed with " << r << " "
			<< "'" << strerror(errno) << "'" << std::endl;
	}
#endif
	return result;
}

std::string
SafeEnv::result2str(Result r)
{
	switch (r) {
#define A(n) case Result::n: return #n;
		A(Success);
		A(Crash);
		A(Timeout);
		A(Error);
		default: return "???";
	}
}