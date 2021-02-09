#ifndef H_CODEGEN_PASSES
#define H_CODEGEN_PASSES

struct Program;
struct Codegen;
struct QueryConfig;
struct PassPipeline;

struct CodegenPassPipeline {
	Codegen& codegen;
	QueryConfig& config;
	PassPipeline* passes;

	void operator()(Program& p);

	CodegenPassPipeline(Codegen& cg, QueryConfig& conf);
	~CodegenPassPipeline();
};

#endif