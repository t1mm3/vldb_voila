#ifndef H_PRINTING_PASS
#define H_PRINTING_PASS

#include "pass.hpp"
#include <sstream>

struct PrintingPass : Pass {
	size_t indent_level = 0;
	size_t indent_spaces = 2;
	std::string indent_string = " ";

	std::ostringstream out;

	void gen_ident(std::ostringstream& out);

	void gen_ident();

	template<typename T>
	void scope(T&& f) {
		indent_level++;
		f();
		indent_level--;
	}

	void on_program(Program& p) override;
	void on_pipeline(Pipeline& p) override;
	void on_lolepop(Pipeline& p, Lolepop& l) override;
	void on_statement(LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(LolepopCtx& ctx, ExprPtr& s) override;
	void on_data_structure(DataStructure& d) override;

	std::string str();

	PrintingPass(bool direct_stdout = false);

	bool direct_stdout;
};


#endif