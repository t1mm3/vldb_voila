#include "codegen_passes.hpp"
#include "pass.hpp"
#include "typing_pass.hpp"
#include "restrictgen_pass.hpp"
#include "printing_pass.hpp"
#include "propagate_predicates_pass.hpp"
#include "flatten_statements_pass.hpp"
#include "runtime_framework.hpp"

void
CodegenPassPipeline::operator()(Program& p)
{
	(*passes)(p);
}

CodegenPassPipeline::CodegenPassPipeline(Codegen& cg, QueryConfig& qconf)
 : codegen(cg), config(qconf)
{
	passes = new PassPipeline();
	auto& p = passes->passes;

	p.emplace_back(std::make_unique<FlattenStatementsPass>());
	p.emplace_back(std::make_unique<DisambiguatePass>());
	p.emplace_back(std::make_unique<TypingPass>(codegen, config));

	p.emplace_back(std::make_unique<LimitVariableLifetimeCheck>());
	// p.emplace_back(std::make_unique<PrintingPass>(true));


	p.emplace_back(std::make_unique<RestrictGenPass>());
	p.emplace_back(std::make_unique<LoopReferences>());
	p.emplace_back(std::make_unique<AnnotateVectorCardinalityPass>());
	p.emplace_back(std::make_unique<CaptureBlendCrossingVariables>());
	p.emplace_back(std::make_unique<FirstDataTouchPass>());
}

CodegenPassPipeline::~CodegenPassPipeline()
{
	delete passes;
}