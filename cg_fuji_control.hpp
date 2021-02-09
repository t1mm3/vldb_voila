#ifndef H_CG_FUJI_CONTROL
#define H_CG_FUJI_CONTROL

#include <string>
#include <vector>
#include <memory>
#include "clite.hpp"

struct FujiCodegen;

struct VarDeclaration {
	const std::string type;
	const std::string name;
	const std::string ctor;
	std::string decl_prologue = "";
	std::string decl_epilogue = "";

	std::string ctor_prologue = "";
	std::string ctor_epilogue = "";

	std::string comment = "";

	VarDeclaration(const std::string& type, const std::string& name,
		const std::string& ctor = "")
	 : type(type), name(name), ctor(ctor) {

	}
};

typedef std::vector<VarDeclaration> VarDeclarations;

// ======= Flow generators =======

struct FlowGenerator {
	FujiCodegen& codegen;
	const std::string name;

	clite::Fragment fragment;

	struct GenCtx {
		std::string unique_prefix;
		std::ostringstream& decl;
		std::ostringstream& impl;
	};

	clite::BlockPtr new_state(const std::string& dbg_name) {
		return fragment.new_block(dbg_name);
	}

	clite::Block* last_state();

	void generate(GenCtx& ctx);
	void clear();

	FlowGenerator(FujiCodegen& codegen, size_t num_par)
	 : codegen(codegen), name("fsm"), num_parallel(num_par) {
	}

	virtual ~FlowGenerator();

	virtual bool is_parallel() const {
		return num_parallel > 1;
	}

	const size_t num_parallel;
protected:
	clite::Block* m_last_state = nullptr;
};

#endif
