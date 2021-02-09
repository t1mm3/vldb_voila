#ifndef H_CG_FUJI_DATA
#define H_CG_FUJI_DATA

#include "voila.hpp"
#include "clite.hpp"
#include <string>
#include <memory>
#include <functional>

struct Expression;
struct Statement;
struct FujiCodegen;
struct IntrinsicTable;
struct DataGen;

// Generated expression
struct DataGenExpr {
	clite::VarPtr var;
	
	bool predicate;
	std::string src_data_gen;
	DataGen& data_gen;

	DataGenExpr(DataGen& gen, const clite::VarPtr& v, bool predicate);

	virtual ~DataGenExpr() {}

	virtual clite::ExprPtr get_len() {
		clite::Factory f;
		return f.literal_from_int(1);
	}
};

typedef std::shared_ptr<DataGenExpr> DataGenExprPtr;

struct QueryConfig;
struct BlendConfig;
struct BlendContext;
struct DataGenBufferCol;
typedef std::shared_ptr<DataGenBufferCol> DataGenBufferColPtr;

struct DataGenBuffer {
	DataGenBuffer(const QueryConfig& qconf, const BlendConfig& in, const BlendConfig& out,
		const std::string& c_name, const std::string& dbg_name);

	size_t buffer_size;
	size_t high_water_mark;
	size_t low_water_mark;

	size_t read_granularity;
	size_t write_granularity;

	const std::string c_name;
	clite::VarPtr var;

	const BlendConfig& in_config;
	const BlendConfig& out_config;

	std::vector<DataGenBufferColPtr> columns;
	std::string parent_class; //! Set during first 'write' into buffer

	const std::string dbg_name;
};

typedef std::shared_ptr<DataGenBuffer> DataGenBufferPtr;

struct State;

struct DataGenBufferPos {
	clite::VarPtr var;
	DataGenBuffer* buffer;
	clite::Block* flush_state;
	clite::ExprPtr num;

	
	DataGenExprPtr pos_num_mask; //!< produced mask
	const std::string dbg_flush;

	DataGenBufferPos(DataGenBuffer* buffer, const clite::VarPtr& var,
		clite::Block* flush_state, const clite::ExprPtr& num,
		const std::string& dbg_flush)
	 : var(var), buffer(buffer), flush_state(flush_state), num(num),
	 dbg_flush(dbg_flush) {
	}
};

typedef std::shared_ptr<DataGenBufferPos> DataGenBufferPosPtr;

struct DataGenBufferCol {
	clite::VarPtr var;
	DataGenBuffer* buffer;

	const bool predicate;
	const std::string scal_type;

	DataGenBufferCol(DataGenBuffer* buffer, const clite::VarPtr& var,
		const std::string& scal_type, const bool predicate);
};
typedef std::shared_ptr<DataGenBufferCol> DataGenBufferColPtr;

struct State;

// Generates data-flow
struct DataGen {
	FujiCodegen& m_codegen;
	size_t m_unroll_factor = 1;

	virtual void gen_expr(ExprPtr& e) = 0;
	virtual void gen_pred(ExprPtr& e) = 0;
	virtual void gen_stmt(StmtPtr& e);
	virtual void gen_output(StmtPtr& e) = 0;
	virtual DataGenExprPtr gen_emit(StmtPtr& e, Lolepop* op);
	virtual void gen_extra(clite::StmtList& stmts, ExprPtr& e);

	struct SimpleExprArgs {
		const std::string& id;
		const std::string& type;
		clite::Variable::Scope scope;
		bool predicate;
		const std::string& dbg_name;
	};

	virtual DataGenExprPtr new_simple_expr(const SimpleExprArgs& args) = 0;

	DataGenExprPtr new_simple_expr(const std::string& id, const std::string& type,
			clite::Variable::Scope scope, bool predicate, const std::string& dbg_name) {
		SimpleExprArgs args {id, type, scope, predicate, dbg_name};

		return new_simple_expr(args);
	}

	virtual DataGenExprPtr new_non_selective_predicate() = 0;
	virtual clite::ExprPtr is_predicate_non_zero(const DataGenExprPtr& pred);

	virtual DataGenExprPtr clone_expr(const DataGenExprPtr& e) = 0;
	virtual void copy_expr(DataGenExprPtr& _res, const DataGenExprPtr& _a) = 0;

	size_t get_unroll_factor() const {
		return m_unroll_factor;
	}

	// generate and modify/return cached
	DataGenExprPtr gen_get_expr(ExprPtr& e);
	DataGenExprPtr gen_get_pred(ExprPtr& e);

	const std::string m_name;
	DataGen(FujiCodegen& cg, const std::string& name);
	virtual ~DataGen();

	// buffer handling
	virtual DataGenBufferPosPtr write_buffer_get_pos(const DataGenBufferPtr& buffer,
		clite::Block* flush_state, const clite::ExprPtr& num,
		const DataGenExprPtr& pred, const std::string& dbg_flush);
	virtual DataGenBufferColPtr write_buffer_write_col(const DataGenBufferPosPtr& pos,
		const DataGenExprPtr& expr, const std::string& dbg_name) = 0;
	virtual void write_buffer_commit(const DataGenBufferPosPtr& pos);

	virtual DataGenBufferPosPtr read_buffer_get_pos(const DataGenBufferPtr& buffer,
		const clite::ExprPtr& num, clite::Block* empty_state,
		const std::string& dbg_refill);
	virtual DataGenExprPtr read_buffer_read_col(const DataGenBufferPosPtr& pos,
		const DataGenBufferColPtr& buffer, const std::string& dbg_name) = 0;
	virtual void read_buffer_commit(const DataGenBufferPosPtr& pos);

	void on_buffer_non_empty(const DataGenBufferPtr& buffer, clite::Block* curr_state, clite::Block* next_state,
		const std::string& dbg_name);

	void assert_buffer_empty(const DataGenBufferPtr& buffer, clite::Block* state);

	enum PrefetchType {
		PrefetchBefore,
		None
	};

	virtual PrefetchType can_prefetch(const ExprPtr& expr);

	virtual void prefetch(const ExprPtr& expr, int temporality);

	virtual std::string get_flavor_name() const;

	std::string get_row_type(const std::string& tbl) const {
		return "__struct_" + tbl + "::Row";
	};

	virtual void begin_eval_scope(bool initial_scope = false) {

	}
	virtual void end_eval_scope() {

	}

#if 0
	std::string access_raw_column(const std::string& tbl, const std::string& col,
			const std::string& index) const {
		return "((" + get_row_type(tbl) + "*)" + index + ")->" + col;
	};

	std::string access_column(const std::string& tbl, const std::string& col,
			const std::string& index) const {
		return access_raw_column(tbl, "col_" + col, index);
	};
#endif

	clite::ExprPtr access_column(const std::string& tbl, const std::string& col,
			const clite::ExprPtr& index) {
		clite::Factory f;

		return f.function("ACCESS_ROW_COLUMN", f.function("wrap",
			f.cast(get_row_type(tbl) + "*", index)), f.literal_from_str(col));
	}

	clite::ExprPtr access_table(const std::string& tbl) const {
		clite::Factory f;

		return f.literal_from_str("thread." + tbl);
	};

	ExprPtr get_bucket_index(const ExprPtr& e);

	clite::Fragment& get_fragment();


	virtual void buffer_overwrite_mask(const DataGenExprPtr& c, const DataGenBufferPosPtr& mask);
protected:
	bool buffer_ensure_exists(DataGenBuffer* buffer);
	bool buffer_ensure_col_exists(const DataGenBufferColPtr& col, const std::string& type = "");

protected:
	// modify expression cache
	void put(ExprPtr& e, DataGenExprPtr&& expr);
	DataGenExprPtr get_ptr(const ExprPtr& e);
	std::string unique_id();


	static std::string get_hash_index_code(const std::string& tbl){
		return "thread." + tbl + "->get_hash_index()";
	}

	static std::string get_hash_mask_code(const std::string& tbl){
		return "thread." + tbl + "->get_hash_index_mask()";
	}

	auto get_hash_index_var(const std::string& tbl) {
		clite::Factory f;
		bool created = false;
		auto var = get_fragment().try_new_var(created, "hash_index__" + tbl, "void**",
			clite::Variable::Scope::ThreadWide, false,
			f.literal_from_str(get_hash_index_code(tbl)));

		return var;
	};

	auto get_hash_mask_var(const std::string& tbl) {
		clite::Factory f;
		bool created = false;
		auto var = get_fragment().try_new_var(created, "hash_mask__" + tbl, "u64",
			clite::Variable::Scope::ThreadWide, false,
			f.literal_from_str(get_hash_mask_code(tbl)));

		return var;
	};

	clite::VarPtr new_global_aggregate(const std::string& tpe,
		const std::string& tbl, const std::string& col,
		const std::string& comb_type);
};

#endif
