#include "runtime.hpp"
#include "runtime_framework.hpp"

struct ThisFrame : Framework {
	ThisFrame(int argc, char* argv[]) : Framework(argc, argv) {}
	virtual std::vector<IPipeline*>construct_pipelines(Query& query, int thread, int num_threads) override {

	}
};

int main(int argc, char* argv[])
{
	ThisFrame w(argc, argv);
	return w();
}