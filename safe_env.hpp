#ifndef H_SAFE_ENV
#define H_SAFE_ENV

#include <functional>
#include <string>

struct SafeEnv {
	const bool m_safe_env;
	const int m_timeout;
	const std::string m_share_name;
	const int m_thread_id;

	SafeEnv(const std::string& sharename, int thread_id,
		bool safe = true, int timeout_secs = 0);

	enum Result {
		Success = 0,
		Timeout = 1, // process timed out
		Crash = 2, // process crashed
		Error = 3, // cannot spawn
	};

	Result operator()(const std::function<void()>& safe);

	static std::string result2str(Result r);
};


#endif