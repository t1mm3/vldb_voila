#ifndef H_RELALG_TRANSLATOR
#define H_RELALG_TRANSLATOR

#include "relalg.hpp"
#include "voila.hpp"

#include <unordered_map>

struct Flow {
	typedef std::unordered_map<std::string, size_t> ColumnMapping;

	ColumnMapping col_map;

	void debug_print(const std::string& op);
};

struct QueryConfig;

struct RelOpTranslator : relalg::RelOpVisitor {
	QueryConfig& config;

	void new_pipeline();

	void transl_op(relalg::RelOp& op);

	size_t id_counter = 0;

	std::string new_unique_name(const std::string& prefix = "") {
		return prefix + std::to_string(id_counter++);
	}

	Flow flow;

	size_t lolepop_id_counter = 0;

	std::string lolepop_name(relalg::RelOp& op, const std::string& stage = "");
	virtual void visit(relalg::Scan& op) final;

	virtual void visit(relalg::Project& op) final;

	virtual void visit(relalg::Select& op) final;
	virtual void visit(relalg::HashAggr& op) final;
	virtual void visit(relalg::HashJoin& op) final;

	Pipeline pipe;

	Program prog;

	RelOpTranslator(QueryConfig& config);

	void operator()(relalg::RelOp& op);
};

#endif