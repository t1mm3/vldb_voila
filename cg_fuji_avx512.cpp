#include "cg_fuji_avx512.hpp"
#include "voila.hpp"
#include "runtime.hpp"
#include "utils.hpp"
#include "cg_fuji.hpp"
#include "cg_fuji_control.hpp"

// #define trace1_printf(...) printf(__VA_ARGS__);
#define trace1_printf(...) 

#define EOL std::endl

#define SCAN_PREFETCH

static const std::string kMaskType = "__mmask8";
static const std::string kMaskFull = "0xFF";
static const std::string kMaskFullGen = "kMaskFull";

Avx512DataGen::SimdExpr*
Avx512DataGen::get(const ExprPtr& e)
{
	LOG_TRACE("Avx512DataGen::get(%p)\n", e.get());
	ASSERT(e);
	auto p = get_ptr(e);
	if (!p) {
		LOG_ERROR("Avx512DataGen::get(%p): Pointer is NULL\n",
			e.get());
		// ASSERT(p);
		return nullptr;
	}
	auto r = dynamic_cast<Avx512DataGen::SimdExpr*>(p.get());
	if (!r) {
		LOG_ERROR("Not castable to Avx512DataGen::SimdExpr: this='%s', p='%s'\n",
			get_flavor_name().c_str(), p->src_data_gen.c_str());
		ASSERT(r && "Must be castable to DataGen's type");
	}
	return r;
}

Avx512DataGen::Mask::Mask(DataGen& dgen, const clite::VarPtr& var)
  : SimdExpr(dgen, var, true) {

}

DataGenExprPtr
Avx512DataGen::new_simple_expr(const DataGen::SimpleExprArgs& args)
{
	const std::string vid(args.id.empty() ? unique_id() : args.id);

	auto var = get_fragment().new_var(vid, args.type, args.scope);	
	if (!var) {
		return nullptr;
	}
	// bool pred = !var->type.compare(kMaskType);
	if (args.predicate) {
		var->type = kMaskType;
	}
	// ASSERT(predicate == pred);

	auto ptr = std::make_shared<SimdExpr>(*this, var, args.predicate);
	ptr->scalar_type = var->type;
	return ptr;
}

DataGenExprPtr
Avx512DataGen::new_non_selective_predicate()
{
	clite::Factory f;
#if 0
	auto var = get_fragment().new_var(unique_id(), kMaskType,
		clite::Variable::Scope::Local, false, kMaskFull);
#endif
	DataGen::SimpleExprArgs args {"", kMaskType, clite::Variable::Scope::Local,
		true, "avx512 new_non_selective_predicate"};
	auto r = new_simple_expr(args);

	r->var->default_value = f.literal_from_str(kMaskFull);

	return r;
}

DataGenExprPtr
Avx512DataGen::clone_expr(const DataGenExprPtr& _a)
{
	auto& fragment = get_fragment();

	ASSERT(_a);
	auto a = std::dynamic_pointer_cast<SimdExpr>(_a);

	auto n = fragment.clone_var(unique_id(), a->var);

	auto r = std::make_shared<SimdExpr>(*this, n, a->predicate);	
	r->scalar_type = a->scalar_type;

	if (a->mask) {
		auto m = fragment.clone_var(unique_id(), a->mask->var);

		r->mask = std::make_shared<Mask>(*this, m);

		r->scalar_type = "u8";
	}
	return r;
}

void
Avx512DataGen::copy_expr(DataGenExprPtr& _res, const DataGenExprPtr& _a)
{
	ASSERT(_res && _a);
	auto a = std::dynamic_pointer_cast<SimdExpr>(_a);
	auto res = std::dynamic_pointer_cast<SimdExpr>(_res);

	ASSERT(a);
	ASSERT(res);

	auto& state = *m_codegen.get_current_state();
	clite::Builder builder(state);
	
	builder << builder.assign(res->var, builder.reference(a->var));

	if (a->mask) {
		builder << builder.comment("clone mask")
			<< builder.assign(res->mask->var, builder.reference(a->mask->var));
	}

	ASSERT(res.get() != a.get());
	ASSERT(res->var.get() != a->var.get());
}

static bool
check_simdizable(const std::string& res_type0)
{
	trace1_printf("%s\n", res_type0.c_str());
	const TypeCode res_type0_code = type_code_from_str(res_type0.c_str());

	if (!type_is_native(res_type0_code)) {
		return false;
	}

	size_t bits = res_type0_code != TypeCode_invalid ?
		8*type_width_bytes(res_type0_code) : 0;

	if (bits == 8) {
		return false;
	}

	return true;
}

static bool
check_simdizable(Expression& e)
{
	const auto arity = e.props.type.arity.size();
	const std::string res_type0(arity > 0 ? e.props.type.arity[0].type : std::string(""));
	return check_simdizable(res_type0);
}

static std::string
simd_type_postfix(size_t bits)
{
	switch(bits) {
	case 64: return std::string("_epi64");
	case 32: return std::string("_epi32");
	case 16: return std::string("_epi16");
	case 8: return std::string("_epi8");

	default:
		ASSERT(false);
		return "";
	}
}

struct IntrContext {
	size_t bits;
	size_t unroll;

	std::string simd_prefix() const {
		size_t mul = bits * unroll;

		switch(mul) {
		case 512: return std::string("_mm512");
		case 256: return std::string("_mm256");
		case 128: return std::string("_mm");
		default:
			ASSERT(false);
			return "";
		}
	}


	std::string simd_postfix() const {
		size_t mul = bits * unroll;

		switch(mul) {
		case 512: return std::string("_si512");
		case 256: return std::string("_si256");
		case 128: return std::string("_si128");
		default:
			ASSERT(false);
			return "";
		}
	}

	std::string native_simd_type() const {
		size_t mul = bits * unroll;

		switch(mul) {
		case 512: return std::string("__m512i");
		case 256: return std::string("__m256i");
		case 128: return std::string("__m128i");
		default:
			ASSERT(false);
			return "";
		}
	}

	std::string wrapped_simd_type() const {
		size_t mul = bits * unroll;

		bool native = false;
		switch (bits) {
		case 8:
		case 16:
		case 32:
		case 64:
			native = true;
			break;
		default:
			native = false;
			break;
		}

		if (!native) {
			return "";
		}

		return std::string("_v" + std::to_string(mul));
	}
};


static std::string
fbuf(const std::string& type, const std::string& unroll) {
	return std::string("_fbuf<" + type + ", ") + unroll + std::string(">");
}

std::string
Avx512DataGen::get_simd_type(const std::string& res_type0) const {
	const size_t unroll = get_unroll_factor();
	const bool simdzable = check_simdizable(res_type0);
	if (!simdzable) {
		return fbuf(res_type0, std::to_string(unroll));
	}
	const TypeCode res_type0_code = type_code_from_str(res_type0.c_str());

	const size_t bits = res_type0_code != TypeCode_invalid ?
		8*type_width_bytes(res_type0_code) : 0;
	IntrContext intr_ctx = {bits, unroll};
	return intr_ctx.wrapped_simd_type();
}

static auto fbuf(const std::string& type, const IntrContext& ctx) {
	return fbuf(type, std::to_string(ctx.unroll));
}

static bool is_type_fbuf(const std::string& type) {
	return strstr(type.c_str(), "fbuf<") != nullptr;
}

static bool is_type_simd(const std::string& type) {
	return !is_type_fbuf(type);
}


clite::ExprPtr
Avx512DataGen::get_pred(const clite::ExprPtr& mask, int idx)
{
	clite::Factory f;
	return f.function("&", mask,
		f.function("<<", f.literal_from_int(1), f.literal_from_int(idx)));
}

clite::ExprPtr
Avx512DataGen::get_pred(const clite::VarPtr& mask, int idx)
{
	clite::Factory f;
	return get_pred(f.reference(mask), idx);
}

static clite::ExprPtr
access_arg(const std::string& vtype, const std::string& base_type,
	const clite::VarPtr& var, int idx)
{
	clite::Factory f;
	auto in = f.reference(var);
	auto idx_lit = f.literal_from_int(idx);

	if (is_type_simd(vtype)) {
		// ASSERT(vtype != base_type);
		LOG_TRACE("k=%d vtype=%s base_type=%s\n",
			idx, vtype.c_str(), base_type.c_str());
		return f.function("SIMD_REG_GET", in, f.literal_from_str(base_type),
			idx_lit);
	} else if (is_type_fbuf(vtype)) {
		return f.function("SIMD_FBUF_GET", in, idx_lit);
	} else {
		ASSERT(false);
	}
}

clite::ExprPtr
Avx512DataGen::read_arg(const std::string& vtype, const std::string& base_type,
	const clite::VarPtr& var, int idx)
{
	clite::Factory f;
	auto in = f.reference(var);
	
	if (!vtype.compare(kMaskType)) {
		return get_pred(in, idx);
	}

	return access_arg(vtype, base_type, var, idx);
}

clite::ExprPtr
Avx512DataGen::read_arg(ExprPtr& e, int idx)
{
	ASSERT(e);

	const auto arity = e->props.type.arity.size();
	const std::string res_type0(arity > 0 ?
		e->props.type.arity[0].type : std::string(""));

	auto expr = get(e);
	ASSERT(expr);

	ASSERT(expr->var);

	return read_arg(expr->var->type, res_type0, expr->var, idx);
}

clite::StmtPtr
Avx512DataGen::write_arg(const std::string& vtype, const std::string& base_type,
	const clite::VarPtr& var, int idx, const clite::ExprPtr& value)
{
	clite::Factory f;

	if (!vtype.compare(kMaskType)) {
		return f.predicated(value,
			f.assign(var, f.function("|", f.reference(var),
				f.function("<<", f.literal_from_int(1), f.literal_from_int(idx)))));
	}

	auto rhs = access_arg(vtype, base_type, var, idx);
	if (!rhs) {
		return nullptr;
	}

	return f.assign(rhs, value);
}

clite::StmtPtr
Avx512DataGen::write_arg(ExprPtr& e, int idx, const clite::ExprPtr& value)
{
	const auto arity = e->props.type.arity.size();
	const std::string res_type0(arity > 0 ?
		e->props.type.arity[0].type : std::string(""));

	auto expr = get(e);
	ASSERT(expr);

	return write_arg(expr->var->type, res_type0, expr->var, idx, value);
}

static clite::ExprPtr
simd_bcast(size_t bits, const clite::ExprPtr& val) {
	clite::Factory factory;
	IntrContext simd_ctx {bits, 8};
	return factory.function(simd_ctx.simd_prefix() + "_set1_epi" + std::to_string(bits), val);
}

static clite::ExprPtr
simd_bcast(size_t bits, int val) {
	clite::Factory factory;
	return simd_bcast(bits, factory.literal_from_int(val));
}

static clite::ExprPtr
new_zero_source(size_t bits)
{
	return simd_bcast(bits, 0);
}

clite::VarPtr
Avx512DataGen::direct_get_predicate(const ExprPtr& pred, int idx)
{
	if (!pred) {
		return nullptr;
	}

	SimdExpr* p = get(pred);
	return direct_get_predicate(p, idx);
}

clite::VarPtr
Avx512DataGen::direct_get_predicate(const SimdExpr* pred, int idx)
{
	if (!pred) {
		return nullptr;
	}

	clite::VarPtr var = pred->var;
	get_pred(pred->var, idx);

	return direct_get_predicate(var, idx);
}

clite::StmtList
Avx512DataGen::direct_wrap_predicated(const clite::VarPtr& pred,
	const clite::StmtList& sts, int idx)
{
	if (!pred) return sts;

	clite::Factory f;

	return { f.comment("direct_wrap_predicated"),
		f.predicated(get_pred(pred, idx), sts) };
}


clite::StmtList
Avx512DataGen::direct_wrap_predicated(const SimdExpr* pred, const clite::StmtList& sts, int idx)
{
	return direct_wrap_predicated(direct_get_predicate(pred, idx), sts, idx);
}

clite::StmtList
Avx512DataGen::direct_wrap_predicated(const ExprPtr& pred, const clite::StmtList& sts, int idx)
{
	return direct_wrap_predicated(direct_get_predicate(pred, idx), sts, idx);
}


void
Avx512DataGen::gen(ExprPtr& e, bool pred)
{
	bool match = false;
	bool create_local = true;
	bool has_result = true;
	bool new_variable = true;

	const auto& n = e->fun;

	clite::Factory factory;

	auto get_pred_mask = [&] () -> clite::ExprPtr {
		clite::Factory f;
		clite::ExprPtr mask;
		for (auto& arg : e->args) {
			SimdExpr* simd = get(arg);
			if (!simd || !simd->mask) {
				continue;
			}

			mask = f.reference(simd->mask->var);
		} 

		const bool has_pred = !!e->pred;
		if (!has_pred) {
			if (mask) {
				return mask;
			} else {
				return f.literal_from_int(0xFF /*kMaskFull */);
			} 
		}
		auto pred = e->pred;
		SimdExpr* expr = get(pred);
		ASSERT(expr->var->type == kMaskType);

		auto pred_mask = factory.reference(expr->var);
		if (mask) {
			return factory.function("_kand_mask8", mask, pred_mask);
		} else {
			return pred_mask;
		}
	};

	auto wrap_predicate_quick_check = [&] (auto& e, const std::function<void(void)>& t) {
#if 0
		const bool has_pred = !!e->pred;

		if (has_pred) {
			impl << "if (" << get(e->pred)->get_c_id() << " != 0) {" << EOL;
		}
#endif

		t();
#if 0
		if (has_pred) {
			impl << "}" << EOL;
		}
#endif
	};

	trace1_printf("gen %p(%s '%s')\n", &e, e->type2string().c_str(), n.c_str());

	std::string id = unique_id();

	const bool simdzable = check_simdizable(*e);
	const auto arity = e->props.type.arity.size();
	const std::string res_type0(arity > 0 ?
		e->props.type.arity[0].type : std::string(""));
	const TypeCode res_type0_code = type_code_from_str(res_type0.c_str());

	const size_t bits = res_type0_code != TypeCode_invalid ?
		8*type_width_bytes(res_type0_code) : 0;
	const size_t unroll = get_unroll_factor();
	IntrContext intr_ctx = {bits, unroll};
	std::string tpe = intr_ctx.wrapped_simd_type();
	DataGenExprPtr omask;

	if (!res_type0.compare("pred_t")) {
		tpe = kMaskType;
	}

	auto dest_var = get_fragment().new_var(id, tpe,
		clite::Variable::Scope::Local);

	clite::StmtList statements;

	statements.emplace_back(factory.comment(e->fun));

	auto bcast_constant = [&] (auto& dest_var, const auto& type, const auto& expr) {
		// TODO: shouldn't be dependent on this stuff
		bool is_string = !type.compare("varchar");
		dest_var->constant = true; 

		if (simdzable && !is_string) {
			dest_var->default_value = factory.function(tpe+"_from",
				factory.function(intr_ctx.simd_prefix() + "_set1" + simd_type_postfix(bits),
					expr));
		} else {
			tpe = fbuf(res_type0, intr_ctx);
			dest_var->default_value = factory.function("_fbuf_set1<" + res_type0 + ", 8>",
				expr);
		}
	};



	// -------------------- comparison ------------------------------------
	auto comparision = [&] (const std::string& voila, const std::string& c,
			const std::string& avx, size_t arity) {
		if (match || n.compare(voila)) {
			return;
		}

		match = true;

		// simdizable?
		bool use_simd = true;
		size_t bits = 0;
		bool has_string = false;
		for (auto& arg : e->args) {
			const auto arity = arg->props.type.arity.size();
			const std::string res_type0(arity > 0 ?
				arg->props.type.arity[0].type : std::string(""));
			const TypeCode res_type0_code = type_code_from_str(res_type0.c_str());

			if (res_type0 == "varchar") {
				has_string = true;
			}

			bits = res_type0_code != TypeCode_invalid ?
				8*type_width_bytes(res_type0_code) : 0;

			if (!check_simdizable(*arg)) {
				use_simd = false;
				break;
			}
		}

		SimdExpr* a = nullptr;
		SimdExpr* b = nullptr;
		tpe = kMaskType;

		ASSERT(e->args.size() == arity);
		if (arity == 2) {
			a = get(e->args[0]);
			b = get(e->args[1]);
		} else {
			ASSERT(false);
		}

		if (use_simd) {
			IntrContext intr_ctx {bits, unroll};

			statements.emplace_back(
				factory.assign(dest_var,
					factory.function(intr_ctx.simd_prefix() + "_cmp" + avx + "_epi" + std::to_string(bits) + "_mask",
						factory.function("SIMD_GET_IVEC", factory.reference(a->var)),
						factory.function("SIMD_GET_IVEC", factory.reference(b->var))))
			);
		} else {
			auto& arg0 = e->args[0];
			auto& arg1 = e->args[1];

			UnrollFlags flags = 0;

			if (!has_string) {
				flags |= kUnrollOptimistic;
			}

			statements.emplace_back(factory.assign(dest_var,
				factory.literal_from_int(0)));

			unrolled(statements, e, [&] (int k) {
				clite::StmtList r;

				auto res = factory.function(c, read_arg(arg0, k), read_arg(arg1, k));

				r.emplace_back(write_arg(tpe, res_type0, dest_var, k, res));
				return r;
			}, flags);
		}

		match = true;
	};

	comparision("eq", "==", "eq", 2);
	comparision("ne", "!=", "neq", 2);
	comparision("lt", "<", "lt", 2);
	comparision("le", "<=", "le", 2);
	comparision("gt", ">", "gt", 2);
	comparision("ge", ">=", "ge", 2);

	auto logic = [&] (const std::string& voila_name, const std::string& c_infix_op,
			const std::string& avxop) {
		if (match || n.compare(voila_name)) return;

		auto& arg0 = e->args[0];
		auto& arg1 = e->args[1];

		auto p0 = get(arg0);
		auto p1 = get(arg1);

		statements.emplace_back(factory.assign(dest_var,
			factory.function(avxop,
				factory.reference(p0->var),
				factory.reference(p1->var))));

		tpe = kMaskType;
		match = true;
	};

	logic("and", "&&", "_kand_mask8");
	logic("or", "||", "_kor_mask8");


	if (!match && !n.compare("contains")) {
		statements.emplace_back(factory.assign(dest_var, factory.literal_from_int(0)));
		tpe = kMaskType;

		unrolled(statements, e, [&] (int k) {
			auto val = factory.function("contains",
				read_arg(e->args[0], k),
				read_arg(e->args[1], k));
			auto write = write_arg(tpe, res_type0, dest_var, k, val);
			return clite::StmtList { write };
		});

		match = true;
	}

	// -------------------- predicate ------------------------------------
	{
		bool predicate_fix_mask = false;
		bool pred_op_match = false;

		auto sel_arg = [&] (const ExprPtr& e) -> clite::ExprPtr {
			auto r = get(e);

			ASSERT(!tpe.compare(kMaskType));
			// ASSERT(!r->var->type.compare(kMaskType) && "Must be mask");

			auto input = factory.reference(r->var);
			if (!r->var->type.compare(kMaskType)) {
				return input;
			}

			// sometimes BLEND/buffer produces fbuf<u8,8>, in case case we need to translate that
			// to kMaskType
			auto new_var = get_fragment().new_var(unique_id(), kMaskType,
				clite::Variable::Scope::Local);

			statements.emplace_back(factory.effect(
				factory.function("translate_pred_avx512__from_scalar",
					factory.reference(new_var), input)));			
			return factory.reference(new_var);
		};

		if (!match && !n.compare("seltrue")) {
			statements.emplace_back(factory.assign(dest_var,
				sel_arg(e->args[0])));

			predicate_fix_mask = true;
			match = true;
			pred_op_match = true;
		}

		if (!match && !n.compare("selfalse")) {
			statements.emplace_back(factory.assign(dest_var,
				factory.function("_knot_mask8", sel_arg(e->args[0]))));

			predicate_fix_mask = true;
			match = true;	
			pred_op_match = true;
		}

		if (!match && !n.compare("selunion")) {
			auto a = sel_arg(e->args[0]);
			auto b = sel_arg(e->args[1]);

			statements.emplace_back(factory.assign(dest_var,
				factory.function("_kor_mask8", a, b)));

			predicate_fix_mask = true;
			match = true;	
			pred_op_match = true;
		}

#if 0
		if (pred_op_match) {
			statements.emplace_back(factory.log_debug(n + " mask = %d",
				{factory.reference(dest_var)}));
		}
#endif
		if (pred_op_match && predicate_fix_mask && e->pred) {
			statements.emplace_back(factory.assign(dest_var,
				factory.function("_kand_mask8",
					factory.reference(get(e->pred)->var),
					factory.reference(dest_var))));

#if 0
			statements.emplace_back(factory.log_debug(n + ":FIX mask = %d",
				{factory.reference(dest_var)}));
#endif
		}
	}
	// -------------------- table ------------------------------------
	{
		bool table_in = false;
		bool table_out = false;

		if (!match && e->is_table_op(&table_out, &table_in)) {
			std::string tbl, col;
			e->get_table_column_ref(tbl, col);

			auto null = factory.literal_from_str("nullptr");
			auto one = factory.literal_from_int(1);

			auto get_row_type = [&] (const auto& tbl) {
				return "__struct_" + tbl + "::Row";
			};

			if (e->is_aggr()) {
				auto& idx = e->args[1];
				has_result = false;
				new_variable = false;

				unrolled(statements, e, [&] (int k) {
					clite::ExprPtr val;

					std::string type;

					if (!n.compare("aggr_count")) {
						type = "COUNT";
						val = one;
					} else {
						val = read_arg(e->args[2], k);
						if (!n.compare("aggr_sum")) {
							type = "SUM";
						} else if (n == "aggr_min") {
						type = "MIN";
						} else if (n == "aggr_max") {
							type = "MAX";
						} else {
							ASSERT(false);
						}
					}

					clite::ExprPtr col_data = factory.cast(get_row_type(tbl) + "*", read_arg(idx, k)); 

					return clite::StmtList { factory.effect(factory.function("SCALAR_AGGREGATE", {
						factory.literal_from_str(type),
						factory.function("ACCESS_ROW_COLUMN",
							col_data,
							factory.literal_from_str("col_" + col)),
						val
					})) };
				});

				match = true;
			} else {

				if (!match && !n.compare("write")) {
					auto& value = e->args[2];
					auto row = get_fragment().new_var(unique_id(), get_row_type(tbl)+"*",
						clite::Variable::Scope::Local);

					statements.emplace_back(factory.assign(row,
						factory.function("ACCESS_BUFFERED_ROW_EX",
							factory.reference(get(e->args[1])->var),
							factory.literal_from_str(get_row_type(tbl)))));

					unrolled(statements, e, [&] (int k) {
						auto val = factory.function("ACCESS_ROW_COLUMN",
							factory.reference(row),
							factory.literal_from_str("col_" + col));
						auto write = factory.assign(val, read_arg(value, k));
						auto increment = factory.assign(row, factory.function("+",
							factory.reference(row), one));
						return clite::StmtList { write, increment };
					});
					
					match = true;
					has_result = false;
					new_variable = false;
				}

				if (!match && !n.compare("read")) {
					auto row = get_fragment().new_var(unique_id(), get_row_type(tbl)+"*",
						clite::Variable::Scope::Local);

				
					if (simdzable) {
						// TODO:
					} else {
						tpe = fbuf(res_type0, intr_ctx);
					}

					statements.emplace_back(factory.assign(row,
						factory.function("ACCESS_BUFFERED_ROW_EX",
							factory.reference(get(e->args[1])->var),
							factory.literal_from_str(get_row_type(tbl)))));

					unrolled(statements, e, [&] (int k) {
						auto val = factory.function("ACCESS_ROW_COLUMN",
							factory.reference(row),
							factory.literal_from_str("col_" + col));
						auto write = write_arg(tpe, res_type0, dest_var, k, val);
						auto increment = factory.assign(row, factory.function("+",
							factory.reference(row), one));
						return clite::StmtList { write, increment };
					});
					
					match = true;
				}

				if (!match && str_in_strings(n, {"check", "gather", "scatter"})) {
					auto& idx = e->args[1];
					const bool check = !n.compare("check");
					const bool scatter = !n.compare("scatter");
					const bool gather = !n.compare("gather");

					std::string type_in_table;
					if (check) {
						tpe = kMaskType;
						// type_in_table?
					} if (scatter) {
						has_result = false;
						new_variable = false;
					} else {
						type_in_table = res_type0;
						if (tpe.empty()) {
							tpe = fbuf(res_type0, intr_ctx);
						}
					}

					bool simd = false;
					size_t col_bits = bits;
					size_t res_bits = bits;

					if (check || scatter) {
						ASSERT(e->args.size() > 2);
						const std::string a2_type0(e->args[2]->props.type.arity[0].type);
						const TypeCode code = type_code_from_str(a2_type0.c_str());
						col_bits = 8*type_width_bytes(code);
					}

					if ((check || gather || scatter) && (col_bits == 32 || col_bits == 64)) { 
						simd = true;
					}

#if 0
					// seems slower see #48
					// we can implement sub-word gather using gather and mask
					if ((gather || check) && (col_bits == 16)) {
						simd = true;
					}
#endif
					if (simd) {
						auto mask = get_pred_mask();
						auto source = new_zero_source(col_bits);

						auto offset = factory.function("_mm512_set1_epi64", factory.function("SIMD_TABLE_COLUMN_OFFSET",
							access_table(tbl), factory.literal_from_str(col)));
						auto ptrs = factory.function("_mm512_add_epi64", offset,
							factory.function("SIMD_GET_IVEC", factory.reference(get(idx)->var)));

						clite::StmtPtr stmt;
						if (scatter) {
							auto arg = get(e->args[2])->var;
							stmt = factory.effect(
								factory.function("_mm512_mask_i64scatter_epi" + std::to_string(col_bits), {
									null, mask, ptrs,
									factory.function("SIMD_GET_IVEC", factory.reference(arg)),
									one
								}));
						} else {
							clite::ExprPtr gather = factory.function("_mm512_mask_i64gather_epi" +
								std::to_string(col_bits),
								{ source, mask, ptrs, null, one });

							IntrContext compare_ctx {col_bits, unroll};
							size_t mul = col_bits * unroll;
							if (check) {
								auto arg = get(e->args[2])->var;
								std::string cmp(compare_ctx.simd_prefix() + "_cmpeq_epi"
									+ std::to_string(col_bits) + "_mask");
								stmt = factory.assign(dest_var, factory.function(cmp, gather,
									factory.function("SIMD_GET_IVEC", factory.reference(arg))));
							} else {
								std::string fun("_v" + std::to_string(mul) + "_from");
								stmt = factory.effect(factory.function(fun, factory.reference(dest_var),
									gather));
							}
						}

						statements.emplace_back(stmt);
					} else {
						statements.emplace_back(factory.comment("not simdzable. res_bits = " +
							std::to_string(res_bits)));
						statements.emplace_back(factory.comment("not simdzable. col_bits = " +
							std::to_string(col_bits)));
						if (check) {
							statements.emplace_back(factory.assign(dest_var, factory.literal_from_int(0)));
						}

						unrolled(statements, e, [&] (int k) {
							clite::StmtPtr r;

							clite::ExprPtr col_data = factory.cast(get_row_type(tbl) + "*",
									read_arg(idx, k));

							col_data = factory.function("ACCESS_ROW_COLUMN", factory.function("NOT_NULL", col_data),
								factory.literal_from_str("col_" + col));

							if (scatter) {
								ASSERT(e->args.size() == 3);
								r = factory.assign(col_data, read_arg(e->args[2], k));
							} else {
								clite::ExprPtr value;
								if (check) {
									ASSERT(e->args.size() == 3);
									value = read_arg(e->args[2], k);
									value = factory.function("==", value, col_data);
								} else {
									ASSERT(e->args.size() == 2);
									value = col_data;
								}
								r = write_arg(tpe, res_type0, dest_var, k, value);
							}
							return clite::StmtList { r };
						});

					}

					match = true;
				}

				if (!match && !n.compare("bucket_lookup")) {
					auto mask = get_pred_mask();
					auto source = new_zero_source(64);
					const auto& idx = get(e->args[1])->var;

					auto hmask = factory.reference(get_hash_mask_var(tbl));
					auto hindex = factory.reference(get_hash_index_var(tbl));

					auto buckets = factory.function("_mm512_and_epi64", factory.function("_mm512_set1_epi64", hmask),
						factory.function("SIMD_GET_IVEC", factory.reference(idx)));

					statements.emplace_back(factory.effect(factory.function("_v512_from",
						factory.reference(dest_var),
						factory.function("_mm512_mask_i64gather_epi64", 
							{ source, mask, buckets, hindex, factory.literal_from_str("sizeof(u64)") }))));

					ASSERT(tpe.size() > 0);
					match = true;
				}

				if (!match && !n.compare("bucket_next")) {
					auto mask = get_pred_mask();
					auto source = new_zero_source(64);
					const auto& idx = get(e->args[1])->var;

					auto offset = factory.function("SIMD_TABLE_NEXT_OFFSET",
						access_table(tbl));
					auto ptrs = factory.function("_mm512_add_epi64",
						factory.function("SIMD_GET_IVEC", factory.reference(idx)),
						factory.function("_mm512_set1_epi64", offset));

					auto gather = factory.function("_mm512_mask_i64gather_epi64", {
						source, mask, ptrs, null, one
					});

					statements.emplace_back(factory.effect(
						factory.function("_v512_from", factory.reference(dest_var), gather)));

					ASSERT(tpe.size() > 0);
					match = true;
				}

				if (!match && !n.compare("bucket_insert")) {
					auto& idx = e->args[1];

					statements.emplace_back(factory.effect(
						factory.function("SIMD_BUCKET_INSERT", {
							factory.reference(dest_var),
							factory.literal_from_str(get_row_type(tbl)),
							access_table(tbl),
							factory.reference(get(e->pred)->var),
							factory.reference(get(idx)->var) })));

					ASSERT(tpe.size() > 0);

					auto hash_index = get_hash_index_var(tbl);
					auto hash_mask = get_hash_mask_var(tbl);

					statements.emplace_back(
						factory.assign(factory.reference(hash_index),
							factory.literal_from_str(get_hash_index_code(tbl))));
					statements.emplace_back(
						factory.assign(factory.reference(hash_mask),
							factory.literal_from_str(get_hash_mask_code(tbl))));

					
					match = true;
				}
			}
		}
	} // table op

	if (!match && !n.compare("scan")) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		auto index = get(e->args[1]);
		ASSERT(index);

		auto scan_ptr = get_fragment().new_var(unique_id(), res_type0 + "*",
			clite::Variable::Scope::ThreadWide, false,
			"(" + res_type0 + "*)(thread." + tbl + "->col_" + col + ".get(0))");

		auto ptr = factory.function("ADDRESS_OF", factory.array_access(factory.reference(scan_ptr),
			factory.function("POSITION_OFFSET", factory.reference(index->var))));

		if (simdzable) {
			const std::string fun(intr_ctx.simd_prefix() + "_loadu" + intr_ctx.simd_postfix());
			statements.emplace_back(
				factory.effect(factory.function(tpe + "_from",
					factory.reference(dest_var),
					factory.function(fun, factory.cast(intr_ctx.native_simd_type() + "*", ptr)))));
		} else {
			tpe = fbuf(res_type0, intr_ctx);

			statements.emplace_back(factory.assign(dest_var,
				factory.function("DEREFERENCE", factory.cast(tpe + "*", ptr))));
		}

		omask = index->mask;
		match = true;
	}

	if (!match && e->type == Expression::Constant) {
		auto constant = e->fun;
		const auto& type = e->props.type.arity[0].type;
		bool is_string = !type.compare("varchar");

		auto val = is_string ? factory.str_literal(constant)
				: factory.literal_from_str(constant);
		bcast_constant(dest_var, type, val);

		match = true;
	}

	if (!match && (!n.compare("hash") || !n.compare("rehash"))) {
		size_t idx = 0;
		bool rehash = false;
		if (!n.compare("rehash")) {
			idx = 1;

			rehash = true;
		}

		const auto& type = e->args[idx]->props.type.arity[0].type;

		UnrollFlags flags;

		if (type != "varchar") {
			flags |= kUnrollOptimistic;
			flags |= kUnrollNoAdaptive;
		}

		const TypeCode tcode = type_code_from_str(type.c_str());

		const size_t bits = tcode != TypeCode_invalid ?
			8*type_width_bytes(tcode) : 0;

		auto& arg0 = e->args[0];
		auto arg1 = rehash ? e->args[1] : nullptr;

		auto p0 = factory.reference(get(arg0)->var);
		auto p1 = rehash ? factory.reference(get(arg1)->var) : nullptr;

		if (bits == 16 || bits == 32 || bits == 64) {
			statements.emplace_back(
				factory.effect(factory.function(tpe + "_from",
					factory.reference(dest_var),
					factory.function("avx512_voila_hash",
						rehash ?
							clite::ExprList {factory.function("SIMD_GET_IVEC", p1), factory.function("SIMD_GET_IVEC", p0)} :
							clite::ExprList {factory.function("SIMD_GET_IVEC", p0)})))
			);
		} else {
			const std::string fun("voila_hash<"
					+ type + ">::"
					+ e->fun);

			unrolled(statements, e, [&] (int k) {
				clite::ExprList args;
				for (size_t i=0; i<e->args.size(); i++) {
					args.emplace_back(read_arg(e->args[i], k));
				}

				auto r = factory.function(fun, args);
				return clite::StmtList { write_arg(tpe, res_type0, dest_var,
					k, r) };
			}, flags);
		}

		match = true;
	}

	auto binary_math = [&] (const std::string& voila_name,
			const std::string& c_infix_op) {
		if (match || n.compare(voila_name)) return;

		bool simd = simdzable;

		std::string simd_name = voila_name;
		if (!simd_name.compare("mul")) {
			simd_name = "mullo";

			simd &= bits == 32 || bits == 64;
		}
		auto& arg0 = e->args[0];
		auto& arg1 = e->args[1];

		auto p0 = factory.reference(get(arg0)->var);
		auto p1 = factory.reference(get(arg1)->var);

		UnrollFlags flags = 0;
		flags |= kUnrollOptimistic;

		simd = false;
		if (simd) {
			statements.emplace_back(
					factory.effect(factory.function(tpe + "_from",
						factory.reference(dest_var),
						factory.function(intr_ctx.simd_prefix() + "_" + simd_name + simd_type_postfix(bits),
							p0, p1)))
				);
		} else {
			unrolled(statements, e, [&] (int k) {
				auto r = factory.function(c_infix_op,
					read_arg(arg0, k), read_arg(arg1, k));
				return clite::StmtList { write_arg(tpe, res_type0, dest_var,
					k, r) };
			}, flags);
		}

		match = true;
	};

	binary_math("add", "+");
	binary_math("sub", "-");
	binary_math("mul", "*");

	if (!match) {
		// is cast?
		auto cast2 = split(n, 'T');
		if (cast2.size() > 1 && !cast2[0].compare("cast")) {
			const auto& in_type = e->args[0]->props.type.arity[0].type;
			const TypeCode in_tcode = type_code_from_str(in_type.c_str());

			const size_t in_bits = in_tcode != TypeCode_invalid ?
				8*type_width_bytes(in_tcode) : 0;

			const TypeCode out_tcode = type_code_from_str(cast2[1].c_str());

			const size_t out_bits = out_tcode != TypeCode_invalid ?
				8*type_width_bytes(out_tcode) : 0;

			UnrollFlags flags = 0;

			ASSERT(res_type0 != in_type && "Useless typecast");

			bool simd = false;
			#define DIR(IN, OUT) { \
				bool rule = in_bits == IN && out_bits == OUT; \
				simd |= rule; \
			}

			#define REV(IN, OUT) DIR(IN, OUT);  DIR(OUT, IN);
			REV(16, 64)
			REV(16, 32)
			REV(32, 64)

			#undef DIR
			#undef REV

			bool has_strings;
			if (in_type != "varchar" && res_type0 != "varchar") {
				flags |= kUnrollOptimistic;
				has_strings = false;
			} else {
				simd = false;
				has_strings = true;
			}

			LOG_DEBUG("simd %d, in_type %s, in bits %d, in signed %d, res type %s, res bits %d, res signed %d\n",
				simd, in_type.c_str(), in_bits, in_type_signed,
				res_type0.c_str(), out_bits, out_type_signed);


			auto& arg0 = e->args[0];
			auto get0 = get(arg0);
			auto p0 = factory.reference(get0->var);

			if (false && !has_strings && get0->var->constant) {
				bcast_constant(dest_var, res_type0, factory.cast(res_type0, read_arg(arg0, 0)));
			} else if (simd) {
				std::string simd_name("cvtepi" + std::to_string(in_bits));

				std::string intrinsic;
				intrinsic = "_mm512_" + simd_name + simd_type_postfix(out_bits);

				statements.emplace_back(
					factory.effect(factory.function(tpe + "_from",
						factory.reference(dest_var),
						factory.function(intrinsic,
							factory.function("SIMD_GET_IVEC", p0))))
				);
			} else {
				statements.emplace_back(factory.comment("cannot SIMD from " + in_type + " to " + res_type0));
				unrolled(statements, e, [&] (int k) {
					auto r = factory.cast(res_type0, read_arg(arg0, k));
					return clite::StmtList { write_arg(tpe, res_type0, dest_var, k, r) };
				}, flags);
			}

			match = true;
		}
	}

	// assume some function
	if (!match) {
		printf("tpe=%s res0 %s\n", tpe.c_str(), res_type0.c_str());
		unrolled(statements, e, [&] (int k) {
			clite::ExprList args;
			for (size_t i=0; i<e->args.size(); i++) {
				args.emplace_back(read_arg(e->args[i], k));
			}

			auto r = factory.function(e->fun, args);
			return clite::StmtList { write_arg(tpe, res_type0, dest_var, k, r) };

		});


		match = true;
	}

	printf("dest_var(%p) -> tpe=%s\n", dest_var.get(), tpe.c_str());
	dest_var->type = tpe;

	clite::Builder builder(*m_codegen.get_current_state());
	builder << statements;

	if (match && create_local) {
		if (!has_result) {
			ASSERT(!new_variable);
		}

		if (new_variable) {

		}



		auto ptr = std::make_shared<SimdExpr>(*this, dest_var, res_type0 == "pred_t");
		ptr->mask = omask;

		ASSERT(!res_type0.empty());
		ptr->scalar_type = res_type0;
		put(e, std::move(ptr));

		trace1_printf("produce result for %p(%s:%s): %s\n",
			e.get(), e->type2string().c_str(), e->fun.c_str(), res_type0.c_str());
		return;
	}

	if (!match) {
		ASSERT(false);
	}
	return;
}

void
Avx512DataGen::gen_expr(ExprPtr& e)
{
	gen(e, false);
}

void
Avx512DataGen::gen_pred(ExprPtr& e)
{
	gen(e, true);
}


void
Avx512DataGen::gen_output(StmtPtr& e) {
	ASSERT(e->type == Statement::Emit);

	SimdExpr* pred = e->pred ? get(e->pred) : nullptr;
	if (!pred) {
		pred = e->expr->pred ? get(e->expr->pred) : nullptr;
	}

	auto& child = e->expr;
	ASSERT(child && !child->fun.compare("tappend"));
	SimdExpr* first = get(child->args[0]);

	clite::Builder builder(*m_codegen.get_current_state());

	for (size_t k=0; k<get_unroll_factor(); k++) {
		clite::StmtList statements;

		statements.emplace_back(builder.plain("query.result.begin_line();"));
		for (auto& arg : child->args) {
			statements.emplace_back(builder.effect(builder.function("SCALAR_OUTPUT",
				read_arg(arg, k))));
		}
		statements.emplace_back(builder.plain("query.result.end_line();"));


		if (pred) {
			statements = direct_wrap_predicated(pred, statements, k);
		}

		if (first->mask) {
			statements = direct_wrap_predicated((SimdExpr*)(first->mask.get()),
				statements, k);
		}

		builder << statements;
	}
}

void
Avx512DataGen::gen_extra(clite::StmtList& stmts, ExprPtr& e)
{
	SimdExpr* expr = get(e);

	if (e->is_get_pos()) {
		auto id = unique_id();
		auto tpe(kMaskType);
		auto mask = get_fragment().new_var(id, tpe,
			clite::Variable::Scope::Local);

		clite::Builder builder(*m_codegen.get_current_state());
		builder << builder.assign(mask, builder.function("_mask8_from_num",
			builder.function("POSITION_NUM", builder.reference(expr->var))));

		expr->mask = std::make_shared<Mask>(*this, mask);
		return;
	}

	const auto& n = e->fun;
	clite::Factory factory;
	bool is_global_aggr = false;
	e->is_aggr(&is_global_aggr);
	if (is_global_aggr) {
		std::string tbl, col;
		e->get_table_column_ref(tbl, col);

		auto null = factory.literal_from_str("nullptr");
		auto one = factory.literal_from_int(1);

		auto get_row_type = [&] (const auto& tbl) {
			return "__struct_" + tbl + "::Row";
		};

		clite::StmtList statements;

		const std::string tpe(e->props.type.arity.size() > 0 ?
			e->props.type.arity[0].type : std::string(""));

		std::string arg_type("i64");

		std::string agg_type;
		std::string comb_type;
		if (n == "aggr_gcount") {
			agg_type = "COUNT";
			comb_type = "SUM";
		} else if (n == "aggr_gsum") {
			agg_type = "SUM";
			comb_type = "SUM";
		} else {
			ASSERT(false);
		}

		auto acc = new_global_aggregate(tpe, tbl, col, comb_type);

#if 1
		// count() can use popcount()
		if (n == "aggr_gcount") {
			clite::ExprPtr num;
			auto pred = e->pred;
			if (pred) {
				SimdExpr* expr = get(pred);
				ASSERT(expr->var->type == kMaskType);
				auto pred_mask = factory.reference(expr->var);
				num = factory.function("popcount32", pred_mask);
			} else {
				num = factory.literal_from_int(get_unroll_factor());
			}

			statements.push_back(factory.effect(factory.function("AGGR_SUM",
					factory.reference(acc), num)));

			stmts.push_back(factory.scope(statements));
			return;
		}
#endif
		unrolled(statements, e, [&] (int k) -> clite::StmtList {
			clite::ExprPtr val;

			printf("unoll %s @ %d\n", n.c_str(), k);

			if (n == "aggr_gcount") {
				val = one;
			} else {
				val = read_arg(e->args[1], k);
			}

			return clite::StmtList {
				factory.effect(factory.function("AGGR_" + agg_type,
					factory.reference(acc), val))
			};
		});

		stmts.push_back(factory.scope(statements));

		return;
	}
}

DataGenExprPtr
Avx512DataGen::gen_emit(StmtPtr& e, Lolepop* op)
{
	(void)op;

	DataGenExprPtr new_pred;
	if (e->expr->type == Expression::Type::Function &&
			!e->expr->fun.compare("tappend")) {
		Mask* mask = nullptr;

		for (auto& arg : e->expr->args) {
			SimdExpr* expr = get(arg);

			ASSERT(expr);

			if (!expr->mask.get()) {
				continue;
			}

			if (mask) {
				// ASSERT(mask == expr->mask.get());
			} else {
				mask = (Mask*)expr->mask.get();
			}
		}

		auto pred = get_ptr(e->expr->pred);



		// TODO: buffer tuples

		// create new mask

		if (pred) {
			return pred;
		} else {
			if (mask) {
				new_pred = std::make_shared<SimdExpr>(*this, mask->var, true);
			} else {
				new_pred = new_non_selective_predicate();
			}
		}
	} else {
		ASSERT(false && "todo");
	}

	return new_pred;
}


DataGenBufferPosPtr
Avx512DataGen::write_buffer_get_pos(const DataGenBufferPtr& buffer,
	clite::Block* flush_state, const clite::ExprPtr& num,
	const DataGenExprPtr& pred, const std::string& dbg_flush)
{
	(void)num;
	clite::Factory f;
	return DataGen::write_buffer_get_pos(buffer, flush_state,
		f.literal_from_int(get_unroll_factor()), pred, dbg_flush);
}

DataGenBufferPosPtr
Avx512DataGen::read_buffer_get_pos(const DataGenBufferPtr& buffer,
	const clite::ExprPtr& num, clite::Block* empty, const std::string& dbg_refill)
{
	(void)num;

	clite::Factory factory;
	auto r = DataGen::read_buffer_get_pos(buffer,
		factory.literal_from_int(get_unroll_factor()), empty, dbg_refill);

	auto mask_var = get_fragment().new_var(unique_id(), kMaskType, clite::Variable::Scope::Local);

	clite::Builder builder(*m_codegen.get_current_state());
	builder << builder.assign(mask_var,
		builder.function("_mask8_from_num",
			builder.function("BUFFER_CONTEXT_LENGTH", builder.reference(r->var))));

	r->pos_num_mask = std::make_shared<Mask>(*this, std::move(mask_var));

	return r;
}

static const bool simd_read_write = true;


DataGenBufferColPtr
Avx512DataGen::write_buffer_write_col(const DataGenBufferPosPtr& pos,
	const DataGenExprPtr& _e, const std::string& dbg_name)
{
	LOG_TRACE("write_buffer state=%p\n", m_codegen.get_current_state());

	auto e = std::dynamic_pointer_cast<SimdExpr>(_e);
	ASSERT(e);

	std::string scal_type(e->scalar_type);
	std::string var_type(e->var->type);

	if (var_type == "__mmask8") {
		scal_type = "pred_t";
		LOG_ERROR("Magic fix. Not sure where the empty 'scalar_type' comes from\n");
	}

	LOG_TRACE("type=%s  scalar_type=%s\n", var_type.c_str(), scal_type.c_str());

	ASSERT(scal_type.size() > 0);

	clite::Builder builder(*m_codegen.get_current_state());

	clite::VarPtr input_var(_e->var);

	const bool is_predicate = e->predicate && pos->buffer->in_config != pos->buffer->out_config;

	if (is_predicate) {
		const std::string new_scal_type("pred_t");
		const std::string new_decl_type("_fbuf<" + new_scal_type + ", 8>");

		auto dest_var = get_fragment().new_var(unique_id(), new_decl_type,
			clite::Variable::Scope::Local);

		builder << builder.effect(builder.function(
			"translate_pred_" + get_flavor_name() + "__to_scalar",
			builder.reference(dest_var),
			builder.reference(input_var)));

		scal_type = new_scal_type;
		var_type = new_decl_type;	

		input_var = dest_var;
	}


	LOG_DEBUG("Avx512DataGen::write_buffer_write_col: dbg_name=%s, type=%s\n",
		dbg_name.c_str(), scal_type.c_str());
	auto buffer_var = get_fragment().new_var(unique_id(), scal_type + "*",
			clite::Variable::Scope::ThreadWide);

	auto column = std::make_shared<DataGenBufferCol>(pos->buffer, buffer_var,
		scal_type, e->predicate);

	buffer_ensure_col_exists(column, scal_type);

	pos->buffer->columns.push_back(column);

	auto pos_var = builder.reference(pos->var);
	auto col_var = builder.reference(column->var);
	auto length = builder.function("BUFFER_CONTEXT_LENGTH", pos_var);


	builder << builder.comment("write buffer '" + dbg_name + "'");


	const bool use_simd = check_simdizable(scal_type);

	if (simd_read_write && use_simd) {
		const TypeCode res_type0_code = type_code_from_str(scal_type.c_str());
		size_t bits = res_type0_code != TypeCode_invalid ?
			8*type_width_bytes(res_type0_code) : 0;

		IntrContext intr_ctx {bits, 8};

		const std::string fun(intr_ctx.simd_prefix() + "_storeu" + intr_ctx.simd_postfix());

		auto ptr = builder.function("ADDRESS_OF", builder.array_access(
			col_var,
			builder.function("BUFFER_CONTEXT_OFFSET", pos_var)));

		builder <<
			builder.effect(builder.function(fun,
				builder.cast(intr_ctx.native_simd_type() + "*" ,ptr),
				builder.function("SIMD_GET_IVEC", builder.reference(input_var))));
	} else {
		for (size_t k=0; k<get_unroll_factor(); k++) {
			auto offset = builder.function("+",
					builder.function("BUFFER_CONTEXT_OFFSET", pos_var),
					builder.literal_from_int(k));

			auto value = builder.array_access(col_var, offset);

			auto cond = builder.function("<", builder.literal_from_int(k), length);

			builder << builder.predicated(cond, clite::StmtList {
				builder.effect(builder.function("ASSERT",
					builder.function("BUFFER_CONTEXT_IS_VALID_INDEX",
						builder.literal_from_str(column->buffer->c_name), offset))),

				builder.assign(value, read_arg(var_type, scal_type, input_var, k))
			});
		}
	}

	return column;
}

DataGenExprPtr
Avx512DataGen::read_buffer_read_col(const DataGenBufferPosPtr& pos,
	const DataGenBufferColPtr& column, const std::string& dbg_name)
{
	LOG_TRACE("read_buffer state=%p\n", m_codegen.get_current_state());

	std::string scal_type(column->scal_type);
	std::string tpe(get_simd_type(scal_type));


	if (tpe.empty()) {
		// non SIMDizable
		tpe = scal_type;
		ASSERT(false && "Todo: non simd");
	}

	auto dest_var = get_fragment().new_var(unique_id(), tpe,
		clite::Variable::Scope::Local);

	clite::Builder builder(*m_codegen.get_current_state());

	builder
		<< builder.comment("read_buffer '" + column->var->name + "'" +
			" into '" + dbg_name + "'");

	auto pos_var = builder.reference(pos->var);
	auto col_var = builder.reference(column->var);
	auto length = builder.function("BUFFER_CONTEXT_LENGTH", pos_var);

	LOG_DEBUG("Avx512DataGen::read_buffer_read_col: dbg_name=%s, type=%s, scal_type=%s, predicate=%d\n",
		dbg_name.c_str(), tpe.c_str(), scal_type.c_str(), column->predicate);

	ASSERT(pos->pos_num_mask);
	ASSERT(pos->pos_num_mask->var);

	const bool use_simd = check_simdizable(scal_type);
	const bool is_predicate = column->predicate && pos->buffer->in_config != pos->buffer->out_config;

	if (simd_read_write && use_simd) {
		const TypeCode res_type0_code = type_code_from_str(scal_type.c_str());
		size_t bits = res_type0_code != TypeCode_invalid ?
			8*type_width_bytes(res_type0_code) : 0;

		IntrContext intr_ctx {bits, 8};

		const std::string fun(intr_ctx.simd_prefix() + "_loadu" + intr_ctx.simd_postfix());

		auto ptr = builder.function("ADDRESS_OF", builder.array_access(
			col_var,
			builder.function("BUFFER_CONTEXT_OFFSET", pos_var)));

		builder <<
			builder.effect(builder.function(tpe + "_from",
				builder.reference(dest_var),
				builder.function(fun, builder.cast(intr_ctx.native_simd_type() + "*", ptr))));
	} else {
		builder
			<< unroll(pos->pos_num_mask->var, [&] (int k) {
				auto offset = builder.function("+",
					builder.function("BUFFER_CONTEXT_OFFSET", pos_var),
					builder.literal_from_int(k));

				auto value = builder.array_access(col_var, offset);

				return clite::StmtList {
					builder.effect(builder.function("ASSERT",
						builder.function("BUFFER_CONTEXT_IS_VALID_INDEX",
							builder.literal_from_str(column->buffer->c_name), offset))),

					write_arg(tpe, scal_type, dest_var, k, value)
				};
			});
	}
	if (is_predicate) {
		const std::string new_scal_type("pred_t");
		const std::string new_decl_type(kMaskType);
		const std::string new_id(unique_id());

		scal_type = new_scal_type;
		tpe = new_decl_type;

		auto new_dest_var = get_fragment().new_var(unique_id(), tpe,
			clite::Variable::Scope::Local);

		const std::string translator("translate_pred_" + get_flavor_name() + "__from_"
			+ "scalar"); // pos->buffer->in_config.computation_type);

		builder
			<< builder.effect(
				builder.function(translator,
					builder.reference(new_dest_var),
					builder.reference(dest_var)))
			<< builder.assign(new_dest_var,
				builder.function("_kand_mask8",
					builder.reference(new_dest_var),
					builder.reference(pos->pos_num_mask->var)))
			;

		dest_var = new_dest_var;
	}

	auto ptr = std::make_shared<SimdExpr>(*this, dest_var,
		column->predicate);
	ptr->scalar_type = scal_type;
	return ptr;
}


DataGen::PrefetchType
Avx512DataGen::can_prefetch(const ExprPtr& e)
{
#ifdef SCAN_PREFETCH
	if (e->fun == "scan") {
		if (e->props.forw_affected_scans.size() > 0) {
			return DataGen::PrefetchType::PrefetchBefore;
		}
	}
#endif

	return DataGen::can_prefetch(e);
}

void
Avx512DataGen::prefetch(const ExprPtr& e, int temporality)
{
	const std::string prefetch_data("PREFETCH_DATA" + std::to_string(temporality));
	const auto& n = e->fun;
	clite::Builder<clite::Block> builder(*m_codegen.get_current_state());

	if (!n.compare("scan")) {

		builder << builder.comment("scan prefetcherooooo");

		for (const auto& scan : e->props.forw_affected_scans) {
			std::string tbl, col;
			scan->get_table_column_ref(tbl, col);

			const auto arity = e->props.type.arity.size();
			const std::string res_type0(arity > 0 ?
				e->props.type.arity[0].type : std::string(""));


			auto index = get(e->args[1]);
			ASSERT(index);

			printf("fuji tbl %s col %s\n", tbl.c_str(), col.c_str());

			auto scan_ptr = get_fragment().new_var(unique_id(), res_type0 + "*",
				clite::Variable::Scope::ThreadWide, false,
				"(" + res_type0 + "*)(thread." + tbl + "->col_" + col + ".get(0))");

			auto ptr = builder.function("ADDRESS_OF", builder.array_access(
					builder.reference(scan_ptr),
				builder.function("POSITION_OFFSET", builder.reference(index->var))));

			builder << builder.effect(builder.function(prefetch_data, ptr));
		}
		return;
	}

	bool table_in = false;
	bool table_out = false;

	ASSERT(e->is_table_op(&table_out, &table_in));
	std::string tbl, col;
	e->get_table_column_ref(tbl, col);

	clite::StmtList statements;

	UnrollFlags flags = 0;

	if (n != "bucket_next") {
		flags |= kUnrollOptimistic | kUnrollAdaptive;
	} else {
		flags |= kUnrollNop;
	}

	unrolled(statements, e, [&] (int k) {
		if (!n.compare("bucket_lookup")) {
			auto index = read_arg(e->args[1], k);

			auto hash_mask = get_hash_mask_var(tbl);
			auto hash_index = get_hash_index_var(tbl);

			auto prefetch = builder.function(prefetch_data,
				builder.function("ADDRESS_OF",
					builder.array_access(
						builder.reference(hash_index),
						builder.function("&", index, builder.reference(hash_mask))
					)));

			return clite::StmtList { builder.effect(prefetch) };
		}

		if (!n.compare("bucket_next")) {
			auto idx = get_bucket_index(e);
			auto col_data = builder.cast(get_row_type(tbl) + "*",
					read_arg(idx, k));

			col_data = builder.function("ACCESS_ROW_COLUMN", col_data,
				builder.literal_from_str("next"));

			auto prefetch = builder.function(prefetch_data, col_data);
			return clite::StmtList { builder.effect(prefetch) };
		}

		auto idx = get_bucket_index(e);
		auto col_data = builder.cast(get_row_type(tbl) + "*",
				read_arg(idx, k));

		col_data = builder.function("ADDRESS_OF", builder.function("ACCESS_ROW_COLUMN", col_data,
			builder.literal_from_str(e->props.first_touch_random_access)));

		auto prefetch = builder.function(prefetch_data, col_data);
		return clite::StmtList { builder.effect(prefetch) };
	}, flags);


	builder << statements;
}


std::string
Avx512DataGen::get_flavor_name() const
{
	return "avx512";
}

void
Avx512DataGen::buffer_overwrite_mask(const DataGenExprPtr& _c,
	const DataGenBufferPosPtr& mask)
{
	ASSERT(_c && mask);

	auto c = std::dynamic_pointer_cast<SimdExpr>(_c);
	ASSERT(c);

	// ASSERT(c->mask);

	if (!c->mask) {
		c->mask = mask->pos_num_mask;
		return;
	}

	clite::Builder builder(*m_codegen.get_current_state());
	builder << builder.assign(c->mask->var,
		builder.reference(mask->pos_num_mask->var));
}