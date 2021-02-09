#ifndef H_CODEGEN_VECTOR
#define H_CODEGEN_VECTOR

#include "codegen.hpp"

struct VectorCodegen : Codegen {
private:
	void gen_lolepop(Lolepop& l, Pipeline& p) override;
	void gen_pipeline(Pipeline& p, size_t number) override;

	struct GenCtx {
		size_t current_case;

		GenCtx() {
			current_case = 0;
		}
	};

	void gen(GenCtx& ctx, Expression& e);
	void gen(GenCtx& ctx, Statement& e);

	std::ostringstream init_body;

	std::unordered_map<Expression*, std::string> expr2num;

	// Input columns and lole-pred are special, we track them to avoid emiting "&" in front of expressions
	std::unordered_map<Expression*, std::string> special_col;

	void evaluate(const std::string& n, Expression& expr, Expression* pred,
		bool effect, const std::string& num, const std::string& reason);

	template<typename T>
	static void init_scalar(T& o, const std::string& name, const std::string& type)
	{
		o << "," << name << "(\"" << name << "\", this, \"" << type << "\")" << std::endl;
	}

	std::string num_vector_size; //!< per-lolepop cache of constant scalar for vector size

	std::string const_vector_size() {
		if (num_vector_size.size() > 0) {
			return num_vector_size;
		}

		num_vector_size = unique_id();
		
		// constant vector size
		decl << "VecScalar<sel_t> " << num_vector_size << "; /* const num */" << std::endl;
		init_scalar(init, num_vector_size, "sel_t");
		next << num_vector_size << ".set_value(query.config.vector_size);" << std::endl;

		return num_vector_size;
	}
public:
	VectorCodegen(QueryConfig& config);
};

#endif