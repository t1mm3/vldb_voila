#ifndef H_CODEGEN
#define H_CODEGEN

#include <string>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <stack>

#include "pass.hpp"
#include "voila.hpp"

namespace runtime {
struct Database;
struct Relation;
struct Attribute;
}

struct CgBaseCol {
	runtime::Attribute& rt_ref;

	const std::string source;
	std::string type;
	double dmin;
	double dmax;
	int varlen;
	size_t maxlen;

	CgBaseCol(const std::string& source, runtime::Attribute& a);
};

struct CgBaseTable {
	runtime::Relation& rt_ref;

	const std::string source;

	std::unordered_map<std::string, std::shared_ptr<CgBaseCol>> cols;

	CgBaseTable(const std::string& source, runtime::Relation& r) : rt_ref(r), source(source) {}
};

struct QueryConfig;

struct LocalVar {
	std::string data_ref;
	std::string num_ref;
	bool scalar;
};

struct Codegen : IPass {
	QueryConfig& config;

	std::ostringstream decl;
	std::ostringstream init;
	std::ostringstream next;
	std::ostringstream impl;

	std::stack<std::unordered_map<std::string, LocalVar>> local_vars;

	std::unordered_map<Expression*, std::vector<std::string>> expr2var;

	std::string expr2get0(Expression* e, bool ignore_error = false);
	std::string expr2get0(const std::shared_ptr<Expression>& e, bool ignore_error = false) {
		return expr2get0(e.get(), ignore_error);
	}
	void expr2set0(Expression* e, const std::string& x);

	std::vector<std::string> lolearg;

public:
	std::unordered_map<std::string, std::shared_ptr<CgBaseTable>> base_tables;

	struct TableColumn {
		std::string type;
		std::string name;
		std::string source; //!< Only for BaseColumns
		bool key;
		bool hash;
	};

protected:
	template<typename T>
	static void map_keys_first(const std::vector<TableColumn>& cols, T&& f) {
		for (auto& c : cols) {
			if (c.key) {
				f(c, c.key);
			}
		}

		for (auto& c : cols) {
			if (!c.key) {
				f(c, c.key);
			}
		}	
	}

	static void gen_table(std::ostringstream& out, DataStructure& d,
		const std::string& id, DataStructure::Type t, const std::vector<TableColumn>& cols);
	static void gen_base_table(std::ostringstream& out, const std::string& id, 
		CgBaseTable& t);

	virtual void gen_pipeline(Pipeline& p, size_t number);
	virtual void gen_lolepop(Lolepop& l, Pipeline& p);
	virtual void gen_datastructure(DataStructure& d, Program& p);
	
	bool last_pipeline;

	template<typename T>void wrap_pipeline(Pipeline& p, size_t number, T&& fun) {
		(void)p;
		impl << "void pipeline_" << number << "(size_t offset, size_t num) {" << std::endl;
		fun();
		impl << "}" << std::endl;
	}

	template<typename T>void wrap_lolepop(Lolepop& l, T&& fun) {
		const std::string n = l.name;
		auto begin = [&] (auto& s) {
			s << "// LOLEPOP '" << n << "'" << std::endl;
		};

		auto end = [&] (auto& s) {
			if (!push_driven)
				s << std::endl << std::endl << std::endl;
		};

		local_vars.push({});

		begin(decl);
		begin(init);
		begin(next);
		begin(impl);

		fun();

		end(impl);
		end(decl);
		end(init);
		end(next);

		local_vars.pop();
	}

	std::string gen_get_pos(const std::string& dest, const std::string& name,
		const std::string& offset_var, const std::string& table,
		const std::string& size);

	std::vector<std::string> emit_vars;

	Codegen(QueryConfig& config);
public:

	std::string operator()(Program& p);
};

#endif