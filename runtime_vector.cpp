#include "runtime_vector.hpp"
#include "runtime.hpp"
#include <cstring>
#include <sstream>
#include <dlfcn.h>

IVec*
__get_vector(const GetVectorDebugArgs& args, IVec* v)
{
#if 0
	printf("get_vector '%s' into '%s' at %d with vec=%p first=%p\n",
		args.dbg_src, args.dbg_dst, args.line, v, v ? v->first : nullptr);
#endif
#if 0
	ASSERT(v);
	ASSERT(v->first);
#endif
	return v;
}


VecEvaluator::VecEvaluator(const char *dbg_name)
 : dbg_name(dbg_name)
{
	reset();
}

void VecEvaluator::reset()
{
	LOG_TRACE("reset VecEvaluator '%s'\n", dbg_name);
	eval_round = 0;
}

void
VecOp::init()
{
	// nothing to do
}

sel_t
VecOp::next()
{
	ASSERT(false && "Not implemented");
}

void
VecOp::evaluate_produced_columns(sel_t num, VecExpr* sel, VecEvaluator* eval)
{
	ASSERT(eval);
	LOG_TRACE("%s: produced %d tuples with sel=%p\n",
		dbg_name, num, sel ? sel->vec : nullptr);
	for (auto& c : produced_columns) {
		if (c) {
			c->evaluate(sel, num, this, eval);
		}
	}
}

VecOp::VecOp(VecOp* child, IPipeline& pipeline, const char *dbg_name)
 : child(child), query(pipeline.query), pipeline(pipeline), op_evaluator(dbg_name),
 dbg_name(dbg_name)
{
	pipeline.query.add_resetable(this);
	reset();
}

void
VecOp::reset()
{
	global_aggr_allocated = nullptr;
	current_evaluator = nullptr;
	state = 0;
	op_evaluator.reset();

	for (auto& r : resetables) {
		r->reset();
	}
}

void
VecOp::add_resetable(IResetable* r)
{
	resetables.push_back(r);
}

VecOp::~VecOp()
{
}



#ifdef DEPRECATE
void
IVec::assign(IVec& s)
{
	first = s.first;
	is_constant = s.is_constant;
}

void
IVec::assign(VecExpr& s)
{
	ASSERT(s.vec);
	assign(*s.vec);	
}
#endif

IVec::IVec(const char* dbg_name, Query& q)
 : dbg_name(dbg_name)
{
}

IVec::~IVec()
{
}




AllocatedVector::AllocatedVector(const char* dbg_name, Query& q, size_t width)
 : IVec(dbg_name, q), width(width)
{
	const auto vsize = q.config.vector_size;

	ASSERT(!alloc);
	ASSERT(vsize > 0);

	capacity = vsize;
	alloc = malloc(vsize * width);
	first = alloc;

	memset(first, -1, vsize * width);
}

AllocatedVector::~AllocatedVector()
{
	free(alloc);
}

AllocatedVector*
AllocatedVector::new_from_type(const char* dbg_name, Query& query, const char* t)
{
#define A(tpe, __) if (!strcmp(t, #tpe)) return new Vec<tpe>(dbg_name, query);
	TYPE_EXPAND_ALL_TYPES(A, 0)

	ASSERT(false && "Invalid type");
#undef A
}



Primitives::Primitives(bool parallel)
	: parallel(parallel)
{
	dlhandle = dlopen(NULL, RTLD_LAZY | RTLD_GLOBAL);
	ASSERT(dlhandle);
}

Primitives::~Primitives()
{
	dlclose(dlhandle);
}

static std::string
translate_type(const std::string& t)
{
	return t;
}

template<typename T, typename N, typename R, typename A> static std::string
prim_name(const N& name, const R& res, const A& args, T&& fun)
{
	std::ostringstream pname;

	pname << "vec_" << name << "__" << res << "_";

	for (auto& col : args) {
		pname << "_" << translate_type(fun(col)) << "_col";
	}

	return pname.str();
}

Primitives::primitive_t
Primitives::lookup(const std::string& name, const std::string& res_t,
	const std::vector<std::string>& types)
{
	return lookup(prim_name(name, res_t, types, [] (auto& a) { return a; }));
}

Primitives::primitive_t
Primitives::lookup(const std::string& name, const std::string& res_t,
	const std::vector<VecExpr*>& exprs)
{
	return lookup(prim_name(name, res_t, exprs, [] (auto& expr) {
		ASSERT(expr);
		return expr->type;
	}));
}

Primitives::primitive_t
Primitives::lookup(const std::string& func)
{
	auto cached = [&] () {
		if (cache.find(func) != cache.end()) {
			return cache[func];
		}

		Primitives::primitive_t r;
		*(void **) (&r) = dlsym(dlhandle, func.c_str());
		if (!r) {
			std::cerr << "Cannot find primitive '" << func << "'" << std::endl;
			ASSERT(false && "Couldn't find primitive");
		}

		LOG_DEBUG("lookup(%s)\n", func.c_str());

		set(func, r);
		return r;
	};

	if (!parallel) {
		return cached();
	}

	{ // check cache
		std::shared_lock lock(mutex);

		if (cache.find(func) != cache.end()) {
			return cache[func];
		}
	}

	{ // update cache		
		std::unique_lock lock(mutex);

		return cached();
	}
}

void
Primitives::add(const std::string& func, primitive_t prim)
{
	ASSERT(cache.find(func) != cache.end() && "Must not exist");
	set(func, prim);
}

void
Primitives::set(const std::string& func, primitive_t prim)
{
	cache[func] = prim;
}

VecExpr::VecExpr(const char* dbg_name, VecOp* op, VecExpr::Type expr_type,
	const std::string& type, const std::string& name,
	const std::vector<VecExpr*>& arguments, bool alloc)
 : arguments(arguments), op(op), name(name), type(type),
 type_code(type_code_from_str(type.c_str())), dbg_name(dbg_name), expr_type(expr_type),
 has_allocated_vector(alloc)
{
	reset();

	for (auto& a : arguments) {
		DBG_ASSERT(a);
	}

	if (has_allocated_vector) {
 		vec = AllocatedVector::new_from_type(dbg_name, op->query, type.c_str());
	} else {
		vec = nullptr;
	}
}

void
VecExpr::reset()
{
	eval_round = 0;
}

sel_t
VecExpr::evaluate(sel_t* sel, sel_t num, VecOp* eval_op,
	VecEvaluator* eval_obj, bool strict)
{
	if (!prim) {
		goto cached;
	}
	sel_t r;

	// never cross operator boundaries
	if (eval_op != op) {
		goto cached;
	}

	if (!strict) {
		if (eval_round >= eval_obj->eval_round) {
			goto cached;
		}
		eval_round = eval_obj->eval_round;
	}

	// evaluate children
	for (auto& arg : arguments) {
		r = arg->evaluate(sel, num, eval_op, eval_obj);
		ASSERT(r == num && "todo");
	}

	// evaluate this
	for (size_t i=0; i<arguments.size(); i++) {
		DBG_ASSERT(arguments[i]->vec);
		DBG_ASSERT(arguments[i]->vec->first);
		argv[i] = arguments[i]->vec->first;
	}

	r = prim(sel, num, vec->first, argv[0], argv[1], argv[2], argv[3], argv[4],
		argv[5], argv[6], argv[7]);	

	ASSERT(r <= num);
	if (r != num) {
		LOG_TRACE("evaluate(%s): r=%d num=%d\n", dbg_name, r, num);
	}

	return r;
 cached:
 	return num;
}

VecExpr::~VecExpr()
{
	if (has_allocated_vector) {
		delete vec;
	}
}

VecFunc::VecFunc(const char* dbg_name, VecOp* op, const std::string& type,
	const std::string& name, const std::vector<VecExpr*>& args,
	bool alloc)
 : VecExpr(dbg_name, op, VecExpr::Func, type, name, args, alloc) {
	prim = op->query.primitives->lookup(name, type, args);
}

VecConst::VecConst(const char* dbg_name, VecOp* op, const std::string& type,
	const std::string& val)
 : VecExpr(dbg_name, op, VecExpr::Const, type, "constant", {})
{
	prim = nullptr;
	auto p = op->query.primitives->lookup(name, type, {"varchar"});

	size_t sz = val.size() + 1;
	data = new char[sz];
	memset(data, 0, sz);

	memcpy(data, val.c_str(), sz-1);

	vec->set_constant();
	varchar v(data);

	AllocatedVector* vector = (AllocatedVector*)vec;
	p(NULL, vector->capacity, vec->first, (char*)&v, NULL, NULL, NULL,
		NULL, NULL, NULL, NULL);
}

VecConst::~VecConst()
{
	delete[] data;
}

VecWrap::VecWrap(const char* dbg_name, VecOp* op, const std::string& type,
	IVec* v)
 : VecExpr(dbg_name, op, VecExpr::Wrap, type, "", {}, false)
{
	if (!v) {
		// Cannot reuse VecExpr's vec deallocation, because this vector might be overwritten
		// during assignment. Using VecExpr vec for deallocation would deallocate the
		// overwritten pointer.
		allocated_vector = AllocatedVector::new_from_type(dbg_name, op->query, type.c_str());
		v = allocated_vector;
	}
	vec = v;
	ASSERT(vec);

	if (!v->first) {
		std::cerr << "Vector given to '" << dbg_name <<"' has no ->first" << std::endl;
		ASSERT(v->first);
	}
}

VecWrap::VecWrap(const char* dbg_name, VecOp* op, VecExpr& e)
 : VecExpr(dbg_name, op, VecExpr::Wrap, e.type, "", {}, false)
{
	vec = e.vec;
	ASSERT(vec);
}

VecWrap::~VecWrap()
{
	if (allocated_vector) {
		delete allocated_vector;
	}
}

#if 0
VecWrapScalar::VecWrapScalar(const char* dbg_name, VecOp* op, const std::string& type,
	void* val)
 : VecWrap(dbg_name, op, type, &value_vector), value_vector(op->query)
{
	value_vector.first = val;
}
#endif



void
VecPipeline::run()
{
	ASSERT(sink);
	sel_t r;

	do {
		r = sink->get_next();

		LOG_TRACE("VecPipeline::run(): r=%d\n", r);
		if (r == 0) {
			break;
		}

		if (last) {
			LOG_TRACE("output r=%d tuples\n", r);
			output(r);
		}
	} while (true);
}

void
VecPipeline::output(sel_t num_rows)
{
	if (!query.config.print_result) {
		return;
	}

	// generate output
	size_t num_cols = sink->produced_columns.size();
	sel_t* sel = nullptr;
	if (sink->produced_sel && sink->produced_sel->vec) {
		sel = (sel_t*)sink->produced_sel->vec->first;
		ASSERT(sel);
	}

	if (!num_rows || !num_cols) {
		return;
	}

	for (sel_t row=0; row<num_rows; row++) {
		query.result.begin_line();
		for (size_t col=0; col<num_cols; col++) {
			auto& expr = sink->produced_columns[col];
			ASSERT(expr);

			switch (expr->type_code) {
#define F(tpe, _) case TypeCode_##tpe: { \
						tpe* cast = (tpe*)expr->vec->first; \
						char* str; \
						size_t buffer_size = voila_cast<tpe>::good_buffer_size(); \
						char buffer[buffer_size]; \
						\
						buffer_size = voila_cast<tpe>::to_cstr(str, &buffer[0], \
							buffer_size, sel ? cast[sel[row]] : cast[row]); \
						query.result.push_col(str, buffer_size); \
						break; \
					}
			
			TYPE_EXPAND_ALL_TYPES(F, _)
#undef F

			default:
				ASSERT(false && "invalid type code");
				break;
			}
		}
		query.result.end_line();
	}
}


IFujiAllocatedVector::IFujiAllocatedVector(size_t bytes)
{
	allocated = malloc(bytes);
	SET_FIRST(*this, allocated);
	alloc = allocated;
}

IFujiAllocatedVector::~IFujiAllocatedVector()
{
	free(allocated);
}