#ifndef H_CODEGEN_HYPER
#define H_CODEGEN_HYPER

#include "codegen.hpp"

struct CodeCollector {

	CodeCollector(std::ostringstream& output);

	void flush();

	template<typename T>
	CodeCollector& operator << (const T &t) {
		if (insert_mode != InsertMode::Mixed) {
			flush();
			insert_mode = InsertMode::Mixed;
		}

		buffer << t;
		return *this;
	}

	~CodeCollector();

	void add_predicated_code(const std::string& prologue, const std::string& code,
		const std::string& epilogue);

	void add_comments(const std::vector<std::string>& comments) {
		// ignore insertion mode for this one
		for (const auto& comment : comments) {
			buffer << comment;
		}
	}
private:
	std::ostringstream& output;
	std::ostringstream buffer;


	enum InsertMode {
		PredicatedCode,
		Mixed
	};
	std::string last_predicate_prologue;
	std::string last_predicate_epilogue;
	InsertMode insert_mode = InsertMode::Mixed;

protected:
	bool collapse_same_predicates = false;
};

struct PredicateCollector : CodeCollector {
	PredicateCollector(std::ostringstream& out) : CodeCollector(out) {
		collapse_same_predicates = true;
	}
};

struct HyperCodegen : Codegen {
	bool jit;

	struct GenCtx {
		Lolepop& l;
		Pipeline& p;

		GenCtx(Lolepop& l, Pipeline& p) : l(l), p(p) {}
		GenCtx(const GenCtx& o) : l(o.l), p(o.p) {}
	};

	std::ostringstream local_decl;
	PredicateCollector code;

	bool pipeline_end_gen = false;
	std::ostringstream pipeline_end;

	void gen_lolepop(Lolepop& l, Pipeline& p) override;
	void gen_pipeline(Pipeline& p, size_t number) override;

	std::string lolepred;


	void gen(GenCtx& ctx, Expression& e);
	void gen(GenCtx& ctx, Statement& e);

	void new_decl(const std::string& type, const std::string& id,
			const std::string& init = "", const std::string& prefix = "") {
		local_decl << prefix << type << " " << id;

		if (!init.size()) {
			if (!type.compare("varchar") || !type.compare("Position") ||
					!type.compare("Morsel")) {
				// nothing
			} else {
				local_decl << " = 0";
			}			
		} else {
			local_decl << init;
		}

		local_decl << ";" << std::endl;
	};
public:
	HyperCodegen(QueryConfig& config, bool jit=false);
};

#endif