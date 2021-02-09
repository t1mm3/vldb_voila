#ifndef H_TYPING_PASS
#define H_TYPING_PASS

#include "pass.hpp"
#include <unordered_map>
#include <limits>

struct Codegen;
struct QueryConfig;
struct LolepopCtx;

struct TypingPass : Pass {
	Codegen& codegen;
	QueryConfig& config;

	std::unordered_map<std::string, TypeProps> var_types;
	std::unordered_map<std::string, bool> var_const;
	std::unordered_map<std::string, bool> var_scalar;

	std::unordered_map<std::string, TypeProps> tbl_col_types;

	TypeProps lolepop_arg;
	size_t round;
	size_t rules_applied;

	TypeProps type_function(ExprPtr& s);
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;
	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_pipeline(Pipeline& p) override;

	size_t pipeline_id;

	void operator()(Program& p) override;

	TypingPass(Codegen& cg, QueryConfig& config) : codegen(cg), config(config) {	
	}
};

#endif