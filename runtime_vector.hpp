#ifndef H_RUNTIME_VECTOR
#define H_RUNTIME_VECTOR

#include "runtime.hpp"
#include "runtime_framework.hpp"
#include "runtime_utils.hpp"
#include "runtime_struct.hpp"

// #define DEPRECATE

struct VecExpr;
struct Primitives;
struct Query;
struct QueryConfig;
struct IPipeline;

// #define kVectorSize 1024

struct VecEvaluator : IResetable {
	size_t eval_round;
	const char *dbg_name;

	VecEvaluator(const char *dbg_name);
	virtual void reset() override;
};

struct VecOp : IResetable {
	virtual void init() = 0; // initializes expressions
	virtual sel_t next() = 0; // retrieve next tuples

	i64 state;

	sel_t in_num, out_num;
	VecExpr* in_sel, * out_sel;

	VecOp* child;
	std::vector<VecExpr*> produced_columns;
	VecExpr* produced_sel; // or NULL

	Query& query;
	IPipeline& pipeline;

	VecEvaluator* current_evaluator;
	VecEvaluator op_evaluator;

	sel_t get_next() { return next(); }

	void evaluate_produced_columns(sel_t num, VecExpr* sel, VecEvaluator* eval);


	VecOp(VecOp* child, IPipeline& pipeline, const char *dbg_name);
	virtual ~VecOp();

	virtual void reset() override;
	void add_resetable(IResetable* r);

private:
	const char *dbg_name;
	std::vector<IResetable*> resetables;


protected:
	void* global_aggr_allocated = nullptr;
};

struct VecExpr;

struct IVec {
	void* first = nullptr;

	bool is_constant = false;

	const char* dbg_name;
protected:
	void* alloc = nullptr;

	IVec(const char* dbg_name, Query& q);
public:

#ifdef DEPRECATE
	void assign(IVec& s);
	void assign(VecExpr& s);
#endif

	void* get() {
		return first;
	}

	void set_constant() {
		is_constant = true;
	}

	virtual ~IVec();

};

struct AllocatedVector : IVec {
	size_t width;
	size_t capacity = 0;

	AllocatedVector(const char* dbg_name, Query& q, size_t width);
	virtual ~AllocatedVector();

	static AllocatedVector* new_from_type(const char* dbg_name, Query& query, const char* t);
};

struct BorrowedVector : IVec {
	BorrowedVector(const char* dbg_name, Query& q)
	 : IVec(dbg_name, q) {

	}
};

template<typename T>struct Vec : AllocatedVector {
	Vec(const char* dbg_name, Query& q)
	 : AllocatedVector(dbg_name, q, sizeof(T)) {}

	T* get() {
		return (T*)first;
	}
};

template<typename T>struct TableVec : BorrowedVector {
	TableVec(const char* dbg_name, Query& q) : BorrowedVector(dbg_name, q) {}

	size_t offset = 0;

	void reset_pointer(size_t data_offset) {
		first = (void*)-1;
		offset = data_offset;
	}

	void set_position(Position& p) {
		first = p.data + offset;
	}

	T* get() {
		return (T*)first;
	}
};


#include <shared_mutex>

struct Primitives {
	typedef sel_t (*primitive_t)(sel_t* sel, sel_t num, void *res, void* p1,
		void* p2, void* p3, void* p4, void* p5, void* p6, void* p7, void* p8);

	Primitives(bool parallel = true);
	~Primitives();


	primitive_t lookup(const std::string& name, const std::string& res_t,
		const std::vector<std::string>& types);
	primitive_t lookup(const std::string& name, const std::string& res_t,
		const std::vector<VecExpr*>& exprs);
	primitive_t lookup(const std::string& func);

	void add(const std::string& func, primitive_t prim);
	void set(const std::string& func, primitive_t prim);

private:
	const bool parallel;

	// cache for often used primitives
	mutable std::shared_mutex mutex;
	std::unordered_map<std::string, primitive_t> cache;

	// fallback: symbol lookup
	void* dlhandle;
};

typedef std::vector<VecExpr*> VecArguments;


struct VecExpr : IResetable {
	static constexpr size_t kMaxParams = 8;

	enum Type {
		Func, Const, Wrap
	};
private:
	size_t eval_round = 0;
	void* argv[kMaxParams];

	const std::vector<VecExpr*> arguments;

public:
	VecOp* op;
	const std::string name;
	const std::string type;
	const TypeCode type_code;

	virtual void reset() override;

public:
	sel_t evaluate(sel_t* sel, sel_t num, VecOp* eval_op, VecEvaluator* eval_obj, bool strict = false);

	sel_t evaluate(VecExpr& sel, sel_t num, VecOp* eval_op, VecEvaluator* eval_obj, bool strict = false) {
		sel_t* s = sel.vec ? (sel_t*)sel.vec->first : nullptr;
		return evaluate(s, num, eval_op, eval_obj, strict);
	}

	sel_t evaluate(VecExpr* sel, sel_t num, VecOp* eval_op, VecEvaluator* eval_obj, bool strict = false) {
		if (sel) {
			return evaluate(*sel, num, eval_op, eval_obj, strict);			
		} else {
			return evaluate((sel_t*)nullptr, num, eval_op, eval_obj, strict);
		}
	}


	Primitives::primitive_t prim = nullptr;

	IVec *vec = nullptr;
	const char* dbg_name;

	virtual ~VecExpr();	

	const Type expr_type;
	bool has_allocated_vector; 

protected:
	VecExpr(const char* dbg_name, VecOp* op, Type expr_type, const std::string& type, const std::string& name,
		const VecArguments& arguments, bool alloc = true);
};

struct VecFunc : VecExpr {
	VecFunc(const char* dbg_name, VecOp* op, const std::string& type, const std::string& name,
		const VecArguments& args, bool alloc = true);
};

struct VecConst : VecExpr {
	char* data;

	VecConst(const char* dbg_name, VecOp* op, const std::string& type, const std::string& val);
	~VecConst();
};

struct VecWrap : VecExpr {
	VecWrap(const char* dbg_name, VecOp* op, const std::string& type, IVec* vec = nullptr);
	VecWrap(const char* dbg_name, VecOp* op, VecExpr& e);

	~VecWrap();
private:
	AllocatedVector* allocated_vector = nullptr;
};

template<typename T>
struct VecScalar : VecExpr {
	BorrowedVector value_vector;
	T value;

	VecScalar(const char* dbg_name, VecOp* op, const std::string& type)
	 : VecExpr(dbg_name, op, VecExpr::Wrap, type, "", {}, false),
	   value_vector(dbg_name, op->query) {
	 	vec = &value_vector;
	 	value_vector.first = &value;
	 	value_vector.set_constant();
	}

	void set_value(const T& v) {
		// printf("%s.set_value(%lld)\n", dbg_name, v);
		value = v;
	}


	VecScalar& operator=(const T& other) {
		value = other;
	}

	VecScalar& operator=(const VecScalar<T>& other) {
		return (*this) = other.value;
	}
};

struct VecPipeline : IPipeline {
	VecOp* sink = nullptr;

	virtual void run() override;

	VecPipeline(Query& q, size_t this_thread) : IPipeline(q, this_thread) {
	}

protected:
	void output(sel_t r);
};

struct GetVectorDebugArgs {
	const char* file;
	const int line;
	const char* dbg_dst;
	const char* dbg_src;
};
IVec* __get_vector(const GetVectorDebugArgs& args, IVec* v);
inline static auto _get_vector(const GetVectorDebugArgs& args, VecExpr* v)
{
	return v ? __get_vector(args, v->vec) : nullptr;
}

inline static auto _get_vector(const GetVectorDebugArgs& args, VecExpr& v)
{
	return __get_vector(args, v.vec);
}


#define assign_vector(dest, src) (dest.vec) = _get_vector(GetVectorDebugArgs { __FILE__, __LINE__, #dest, #src}, src)
#define get_vector(src) _get_vector(GetVectorDebugArgs { __FILE__, __LINE__, "", #src}, src)


#define VEC_ARGS(...) __VEC_ARGS({__VA_ARGS__})

static inline std::vector<VecExpr*>
__VEC_ARGS(const std::vector<VecExpr*>& r)
{
	return r;
}

#define VEC_PTR_GET(X) (VecExpr*)((X).get())

inline bool _is_valid(const std::unique_ptr<VecScalar<Morsel>>& p, const char* txt, const char* file, const int line) {
	return _is_valid(p->value, txt, file, line);
}

#define ALLOC_FROM_STATIC_ARRAY(ARRAY) &ARRAY[0] 

struct IFujiVector {
	void* first = nullptr;
	void* alloc = nullptr;

	static void SET_FIRST(IFujiVector& r, void* first) {
		r.first = first;
	}

	static void RESET(IFujiVector& r) {
		r.first = nullptr;
	}

	static void* SELF_GET_FIRST(IFujiVector& r) {
		if (!r.first) {
			r.first = r.alloc;
		}
		return r.first;
	}

	static void* USE_GET_FIRST(IFujiVector& r) {
		return r.first;
	}

	static void const* USE_GET_FIRST(const IFujiVector& r) {
		return r.first;
	}

	template<typename T>
	static void BROADCAST(IFujiVector& r, const T& v, size_t num) {
		T* data = (T*)SELF_GET_FIRST(r);
		for (size_t i=0; i<num; i++) {
			data[i] = v;
		}
	}

	template<typename T>
	static void IDENTITY(IFujiVector& r, size_t num, size_t offset = 0) {
		T* data = (T*)SELF_GET_FIRST(r);
		for (size_t i=0; i<num; i++) {
			data[i] = offset + i;
		}
	}

	template<typename T>
	static T* GET_VALUE_PTR(const IFujiVector& v, size_t i) {
		T* data = (T*)USE_GET_FIRST(v);
		return &data[i];
	}

	template<typename T>
	static T GET_VALUE(const IFujiVector& v, size_t i) {
		return *GET_VALUE_PTR<T>(v, i);
	}

	template<typename T>
	static void OUTPUT(Query& query, const IFujiVector& v, size_t i) {
		auto value = GET_VALUE<T>(v, i);
		__scalar_output<Query, T>(query, value);
	}
};

struct IFujiAllocatedVector : IFujiVector {
protected:
	IFujiAllocatedVector(size_t bytes);
	~IFujiAllocatedVector();

private:
	void* allocated;
};

template<typename T, size_t VectorSize>
struct FujiAllocatedVector : IFujiAllocatedVector {
	FujiAllocatedVector() : IFujiAllocatedVector(sizeof(T[VectorSize])) {
	}

	FujiAllocatedVector(const FujiAllocatedVector&) = delete;
	FujiAllocatedVector& operator=(FujiAllocatedVector const&) = delete;

	~FujiAllocatedVector() {
	}
};

#define STRINGIFY(x) #x

#endif