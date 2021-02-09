#!/bin/python

import sys
import getopt

cardinal_types = ["i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "i128"]
types = cardinal_types + ["varchar", "Position"]


def uniq_name(prefix, name, result, types):
	sign = ""
	i = 0
	for t in types:
		sign = "{}_{}_col".format(sign, t)
		i = i+1

	return "{prefix}_{name}__{result}_{sign}".format(prefix=prefix,
		name=name, result=result, sign=sign)

def should_exploit_full_eval(result, types):
	full_eval_exclude = ["i128"]

	for exclude in full_eval_exclude:
		if exclude == result:
			return False
		if exclude in types:
			return False

	return True

def vec_primitive(name, result, types, case, loop,
		prologue = "", epilogue = "", allow_full_eval = False,
		unroll_nosel=0, unroll_sel=8):
	params = ""
	i = 1
	iter_dict = {"res_t": result}
	for t in types:
		params = "{}, {}* RESTRICT col{}".format(params, t, i)
		iter_dict["col{}_t".format(i)] = t
		i = i+1

	prototype = "VEC_KERNEL_PRE sel_t {unique_name} VEC_KERNEL_IN (sel_t* RESTRICT sel, sel_t inum, {result}* RESTRICT res {params}) VEC_KERNEL_POST".format(
		unique_name=uniq_name("vec", name, result, types), name=name, result=result, params=params)

	full_eval_code = """
	debug_selection_vector_assert_order((sel_t*)sel, inum);
	"""

	if allow_full_eval and should_exploit_full_eval(result, types):
		full_eval_code = full_eval_code + """{{
			if (Vectorized::optimistic_full_eval(sel, inum)) {{
				inum = sel[inum-1]+1;
				sel = nullptr;
			}}
}}"""

	full_eval_code = full_eval_code + """
			/* printf("%s: sel=%p num=%lld\\n", __func__, sel, inum); */
"""

	if case == 1:
		iter_str = loop.format(**iter_dict)
		unroll_sel_code = ""
		unroll_nosel_code = ""

		if unroll_nosel > 0:
			unroll_nosel_code = "for (; k+{n}<inum; k+={n}) {{".format(n=unroll_nosel)
			for i in range (0, unroll_nosel):
				unroll_nosel_code = unroll_nosel_code + """
					{{
						const sel_t i=k+{offset};
						{iter}
					}}
				""".format(iter=iter_str, offset=i)
			unroll_nosel_code = unroll_nosel_code + "}"

		if unroll_sel > 0:
			unroll_sel_code = "for (; k+{n}<inum; k+={n}) {{".format(n=unroll_sel)
			for i in range (0, unroll_sel):
				unroll_sel_code = unroll_sel_code + """
					{{
						const sel_t i=sel[k+{offset}];
						{iter}
					}}
				""".format(iter=iter_str, offset=i)
			unroll_sel_code = unroll_sel_code + "}"

		return """{proto} {{
			VEC_KERNEL_PROLOGUE(sel, inum);
			sel_t k=0;
			sel_t onum = inum;

			{full_eval_code}

			{prologue}

			if (sel) {{
				{unroll_sel_code}

				for (; k<inum; k++) {{
					const sel_t i=sel[k];
					DBG_ASSERT(i >= 0);
					{iter}
				}}
			}} else {{
				{unroll_nosel_code}

				for (; k<inum; k++) {{
					const sel_t i=k;
					{iter}
				}}
			}}

			{epilogue}
			VEC_KERNEL_EPILOGUE(sel, onum);
			return onum;
		}}
		""".format(proto=prototype, iter=iter_str, 
			result=result, prologue=prologue,
			epilogue=epilogue, full_eval_code=full_eval_code,
			unroll_sel_code=unroll_sel_code,
			unroll_nosel_code=unroll_nosel_code)
	elif case == 0:
		return "{};\n".format(prototype)
	else:
		assert(False)
		return ""

def gen_primitive(ctx, name, result, types, loop, *args, **kwargs):
	(h, c) = ctx
	h.write(vec_primitive(name, result, types, 0, loop, *args, **kwargs))
	c.write(vec_primitive(name, result, types, 1, loop, *args, **kwargs))

def is_boolean(t):
	return t == "u8"

def is_index(t):
	return t == "u64"

def is_cardinal(t):
	return t in cardinal_types

def is_sel(t):
	return t == "u32"

def is_one_type(t):
	return is_boolean(t)

def is_meta_type(t):
	return t == "Position"

def is_prim_type(t):
	return not is_meta_type(t)
	
def all_prim_types(res, ts):
	if not is_prim_type(res):
		return False

	for t in ts:
		if not is_prim_type(t):
			return False

	return True

def replace_in_list(ls, repl):
	rs = []

	for l in ls:
		if not l in repl:
			rs.append(l)
			continue

		rs.append(repl[l])

	return rs


def wrap(ctx, result, types):
	assert(len(types) == 6)

	(h, c) = ctx

	# 6 ary

	assert(len(types) > 5)

	if not is_one_type(types[5]):
		return

	types = types[:5]

	if False and all_prim_types(result, types):
		if is_boolean(result) and is_index(types[0]) and types[2] == "u64":
			marker_type = types[1]
			if marker_type == types[3] and marker_type == types[4]:
				gen_primitive(ctx, "mark", result, types, """
					auto idx = col1[i];
					auto m = &col2[idx * stride];
					res[i] = old_val == *m;
					if (res[i]) {{
						*m = new_val;
					}}
					""", prologue="""
					const auto stride = col3[0];
					const auto old_val = col4[0];
					const auto new_val = col5[0];
					""")

	# 5 ary
	if not is_one_type(types[4]):
		return
	types = types[:4]

	if all_prim_types(result, types):
		if is_index(types[2]):
			if is_boolean(result) and is_cardinal(types[1]) and is_index(types[2]):
				if types[0] == types[3]:
					if types[0] == "varchar":
						gen_primitive(ctx, "check", result, types,
							"""
							{col1_t}* RESTRICT data = ({col1_t}* RESTRICT)((char* RESTRICT)col3[i] + offset);
							res[i] = *data == col4[i];

							LOG_TRACE("%s: line %d: check data=%s key=%s\\n", g_pipeline_name, g_check_line, data->as_string().c_str(), col4[i].as_string().c_str());
	""", prologue="""
	const size_t offset = ((ITable::ColDef*)(col2[0]))->offset;
	""", unroll_nosel=8)
					else:
						gen_primitive(ctx, "check", result, types,
							"""
							{col1_t}* RESTRICT data = ({col1_t}* RESTRICT)((char* RESTRICT)col3[i] + offset);
							res[i] = *data == col4[i];

							LOG_TRACE("%s: line %d: check data=%d key=%d\\n", g_pipeline_name, g_check_line, *data, col4[i]);
	""", prologue="""
	const size_t offset = ((ITable::ColDef*)(col2[0]))->offset;

#ifdef __AVX512F__
	if (sizeof(col3[0]) == 8 && sizeof(col4[0]) == 4) {
		return avx512_check__u8__u32_col_u64_col_u64_col_u32_col(sel, inum, res, (u64*)col3, (u32*)col4, offset);
	}
#endif

	""", unroll_nosel=8)

	# 4 ary
	types = types[:4]

	if is_cardinal(types[0]) and is_cardinal(types[1]) and is_index(types[2]) and is_index(types[3]):
		if is_cardinal(result):

			gen_primitive(ctx, "aggr_gsum", result, types,
				"""data += col2[i];""",
				prologue="""
				size_t offset = ((ITable::ColDef*)(col1[0]))->offset;
				void* bucket = *((void**)col3[0]);
				auto data_ptr = (decltype(res) RESTRICT)((char* RESTRICT)bucket + offset);
				auto data = *data_ptr;
				IHashTable* RESTRICT table = (IHashTable*)col4[0]; 
				auto hash_ptr = (u64*)((char* RESTRICT)bucket + table->hash_offset);
				*hash_ptr = kGlobalAggrHashVal;
				""",
				epilogue="*data_ptr = data;")

	if not is_one_type(types[3]):
		return


	# ternary
	types = types[:3]

	if is_cardinal(types[0]) and is_cardinal(types[1]) and is_index(types[2]):
		if is_cardinal(result):
			gen_primitive(ctx, "aggr_gcount", result, types,
				"",
				prologue="""
				size_t offset = ((ITable::ColDef*)(col1[0]))->offset;
				void* bucket = *((void**)col2[0]);
				auto data_ptr = (decltype(res) RESTRICT)((char* RESTRICT)bucket + offset);
				auto data = *data_ptr;
				IHashTable* RESTRICT table = (IHashTable*)col3[0]; 
				auto hash_ptr = (u64*)((char* RESTRICT)bucket + table->hash_offset);
				*hash_ptr = kGlobalAggrHashVal;
				*data_ptr = data + inum;
				return inum;
				""",
				epilogue="")

	if all_prim_types(result, types):
		if is_cardinal(result) and is_index(types[0]) and is_cardinal(types[1]) and is_cardinal(types[2]): # and is_index(types[1]):
			gen_primitive(ctx, "aggr_sum", result, types,
				"""
				DBG_ASSERT(col2[i] != 0);
				DBG_ASSERT(col1[i] >= 0);

				auto data = (decltype(res) RESTRICT)((char* RESTRICT)col2[i] + offset);

				// printf("%s: data=%lld\\n", __func__, (i64)*data);

				*data = *data + col3[i];""",
				prologue="""size_t offset = ((ITable::ColDef*)(col1[0]))->offset;""")
			gen_primitive(ctx, "aggr_count", result, types,
				"""
				/* std::cout << inum << "count @" << (i64)col2[i] << std::endl; */
				DBG_ASSERT(col2[i] != 0);
				DBG_ASSERT(col1[i] >= 0);

				auto data = (decltype(res) RESTRICT)((char* RESTRICT)col2[i] + offset);

				// printf("%s: data=%lld\\n", __func__, (i64)*data);
				*data = *data + 1; (void)col3;""",
				prologue="""size_t offset = ((ITable::ColDef*)(col1[0]))->offset;""")

			if False and types[0] == result:
				gen_primitive(ctx, "aggr_min", result, types,
					"if (col1[i] < res[col2[i] * col1[0]]) {{ res[col2[i] * col1[0]] = col3[i]; }};")
				gen_primitive(ctx, "aggr_max", result, types,
					"if (col1[i] > res[col2[i] * col1[0]]) {{ res[col2[i] * col1[0]] = col3[i]; }};")

		if is_index(types[2]) and is_cardinal(types[1]):
			if types[0] == result:
				gen_primitive(ctx, "gather", result, types,
					"""
					auto data = (decltype(res) RESTRICT)((char* RESTRICT)col3[i] + offset);
					res[i] = *data;""",
					prologue="""size_t offset = ((ITable::ColDef*)(col2[0]))->offset;""")
				if is_cardinal(result):
					gen_primitive(ctx, "read", result, types,
						"""res[i] = data[(col3[0]+index) * stride]; index++;
						//printf("%s: res[%lld]=%lld\\n", __func__, i, (i64)res[i]);
						""",
						prologue="""size_t index=0;
	size_t stride = ((ITable::ColDef*)(col2[0]))->stride;
	size_t offset = ((ITable::ColDef*)(col2[0]))->offset;
	decltype(col1) RESTRICT data = (decltype(col1) RESTRICT)((char* RESTRICT)col1 + offset);
""")
				else:
					gen_primitive(ctx, "read", result, types,
					"""res[i] = data[(col3[0]+index) * stride]; index++;""",
					prologue="""size_t index=0;
	size_t stride = ((ITable::ColDef*)(col2[0]))->stride;
	size_t offset = ((ITable::ColDef*)(col2[0]))->offset;
	decltype(col1) RESTRICT data = (decltype(col1) RESTRICT)((char* RESTRICT)col1 + offset);
""")

		if is_index(types[1]) and is_cardinal(types[0]):
			if types[2] == result:
				gen_primitive(ctx, "scatter", result, types,
					"""
				auto data = (decltype(col3) RESTRICT)((char* RESTRICT)col2[i] + offset);
				*data = col3[i];
				""", prologue="""
				LOG_TRACE("col1(def)=%p col2(column)=%p col3(to_write)=%p\\n",
					col1, col2, col3);
				size_t offset = ((ITable::ColDef*)(col1[0]))->offset;

				LOG_TRACE("%s: offset=%p \\n", __func__, offset);""")

				if result == "varchar":
					gen_primitive(ctx, "write", result, types,
						"""
	LOG_TRACE("%s: %s: %s\\n", g_pipeline_name, __func__, col3[i].as_string().c_str() );
	data[(base+index) * stride] = col3[i]; index++;
						""",
						prologue="""size_t index=0;
	const size_t stride = ((ITable::ColDef*)(col1[0]))->stride;
	const size_t offset = ((ITable::ColDef*)(col1[0]))->offset;
	const auto base = col2[0];
	decltype(res) data = (decltype(res) RESTRICT)((char* RESTRICT)res + offset);
	""", epilogue="/*ASSERT(false);*/")
				else:
					gen_primitive(ctx, "write", result, types,
					"""
LOG_TRACE("%s: %s: %p\\n", g_pipeline_name, __func__, (void*)col3[i]);
data[(base+index) * stride] = col3[i]; index++;
					""",
					prologue="""size_t index=0;
const size_t stride = ((ITable::ColDef*)(col1[0]))->stride;
const size_t offset = ((ITable::ColDef*)(col1[0]))->offset;
const auto base = col2[0];
decltype(res) data = (decltype(res) RESTRICT)((char* RESTRICT)res + offset);
""", epilogue="/*ASSERT(false);*/")
	else:
		# implement new read and write
		if types[2] == "Position" and is_cardinal(types[1]):
			if types[0] == result and is_prim_type(result):
				n = "read"
				proper_types = replace_in_list(types, {'Position' : "u64"})
				proper_prim = uniq_name("vec", n, result, proper_types)
				gen_primitive(ctx, n, result, types, "(void)i; (void)col1;",
					prologue="""
					// printf("%s: inum=%lld num=%lld position=%p col2=%lld\\n", __func__, inum, col3->num, col3, *col2);
					ASSERT(inum == col3->num);
					return {}(sel, inum, res, ({}*)col3->data, col2, (u64*)&col3->offset);""".format(proper_prim, result))

		if types[1] == "Position" and is_cardinal(types[0]):
			if types[2] == result and is_prim_type(result):
				# write
				n = "write"
				proper_types = replace_in_list(types, {'Position' : "u64"})
				proper_prim = uniq_name("vec", n, result, proper_types)
				gen_primitive(ctx, n, result, types, "(void)i;",
					prologue="""
					// printf("%s: %s: inum=%lld num=%lld position=%p col2=%lld\\n", g_pipeline_name, __func__, inum, col3->num, col3, *col2);
					ASSERT(inum == col2->num);
					return {}(sel, inum, ({}*)col2->data, col1, (u64*)&col2->offset, col3);""".format(proper_prim, result))
		pass

	if not is_one_type(types[2]):
		return
	# binary
	types = types[:2]

	if all_prim_types(result, types):
		basic_ops = ("ne", "!="), ("eq", "==")
		if is_boolean(result) and types[0] == types[1]:
			for (name, op) in basic_ops:
				prolog = ""
				if (name == "eq" and (types[0] == "u64" or types[0] == "i64")):
					prolog = """
#ifdef __AVX512F__
					return avx512_vec_eq__u8__u64_col_u64_col(sel, inum, res, (u64*)col1, (u64*)(col2));
#endif
					"""

				gen_primitive(ctx, name, result, types,
					"""res[i] = col1[i] """ + op + """ col2[i];
					/*
					if (!i) LOG_DEBUG("col1=%p col2=%p\\n", col1, col2);
					LOG_DEBUG("%p [%lld] = %lld %lld -> %lld\\n", res, i, col1[i], col2[i], res[i], col1, col2);
					*/
					""",
					allow_full_eval=is_cardinal(types[0]), prologue=prolog)

		if is_boolean(result) and types[0] == types[1] and types[0] == "varchar":
			gen_primitive(ctx, "contains", result, types,
				"res[i] = col1[i].contains(col2[0]);")

		if is_cardinal(types[0]) and is_cardinal(types[1]):
			if is_cardinal(result):
				ops = [("add", "+"), ("sub", "-"), ("mul", "*")]

				for (name, op) in ops:
					gen_primitive(ctx, name, result, types,
						"""res[i] = ({res_t})col1[i] """ + op + """ ({res_t})col2[i];""", allow_full_eval=True)


			ops = [("lt", "<"), ("le", "<="), ("gt", ">"), ("ge", ">="),
				("and", "&&"), ("or", "||")]

			if is_boolean(result) and types[0] == types[1]:
				for (name, op) in ops:
					gen_primitive(ctx, name, result, types,
						"res[i] = col1[i] " + op + " col2[i];", allow_full_eval=True)

		if False:
			if is_sel(result) and types[0] == types[1]:
				for (name, op) in ops:
					gen_primitive(ctx, name, result, types,
						"if (col1[i] " + op + " col2[0]) {{res[onum++] = i; }}",
						prologue="onum=0;", epilogue="""LOG_TRACE("%s: returned %d\\n", __func__, (int)onum);""")		


		if is_index(result) and is_index(types[0]):
			gen_primitive(ctx, "rehash", result, types,
				"""{col2_t} c2 = col2[i];
	res[i] = voila_hash<{col2_t}>::rehash(col1[i], c2);
	""", allow_full_eval=types[0] != "varchar")

		if is_index(result) and types[0] == "u64" and is_cardinal(types[1]):
			#  insert :: (IDX -> HEAD -> u64 -> NEXT -> u64) -> HEAD -> &num_groups
			gen_primitive(ctx, "bucket_insert", result, types, """
		const u64 idx = indices[i] & bucket_mask;

		auto& head = heads[idx];
		res[i] = 0;

		bool conflict = head >= ptr_begin && head <= ptr_end;

		if (!conflict) {{

			res[i] = (u64)((char*)ptr_begin + (row_width*num_inserted));
			nexts[num_inserted * next_stride] = head;

			head = res[i];
			num_inserted++;
		}}

					""",
					prologue="""
		if (!inum) return 0;
		IHashTable* RESTRICT table = (IHashTable*)col1[0]; 

		/* prealloc space for potential new groups */
		Block* block = table->hash_append_prealloc<false>(inum);

		/* to later detect conflicts, determine pointer range inside current block */
		u64 ptr_begin = 0;
		u64 ptr_end = 0;
		table->get_current_block_range((char**)&ptr_begin, (char**)&ptr_end, inum);

		/* set common table stuff */
		const u64 bucket_mask = table->get_hash_index_mask();
		u64* heads = (u64*)table->get_hash_index();
		const auto& indices = col2;
		
		const size_t next_offset = table->next_offset;
		const size_t next_stride = table->next_stride;
		u64* nexts = (u64*)((char*)ptr_begin + next_offset);

		const size_t row_width = table->get_row_width();
		u64 num_inserted = 0;
					""",

					epilogue="""
		table->hash_append_prune<false>(block, num_inserted);

		// LOG_DEBUG("%s: num=%d\\n", __func__,num_inserted);
					""")

			gen_primitive(ctx, "bucket_insert_done", result, types,
				"(void)res; (void)i; (void)col1; (void)col2;",
				prologue="return inum;")

			gen_primitive(ctx, "bucket_lookup", result, types,
				"""
				res[i] = buckets[col2[i] & mask];
				/*
				LOG_DEBUG("%s: i=%lld idx=%lld res=%p\\n", __func__, i, col2[i], res[i]);
				*/
				""",
				prologue="""IHashTable* RESTRICT table = (IHashTable*)col1[0];
				const u64 RESTRICT * buckets = (u64*)table->get_hash_index();
				const u64 mask = table->get_hash_index_mask();

#ifdef __AVX512F__
	if (sizeof(col2[0]) == 8) return avx512_bucket_lookup(sel, inum, res, buckets, (u64*)col2, mask);
#endif

				LOG_TRACE("table=%p bucket=%p mask=%p\\n", table, buckets, mask);
				""")

			gen_primitive(ctx, "bucket_next", result, types,
				"""
				u64* RESTRICT data = (u64* RESTRICT)((char* RESTRICT)col2[i] + next_offset);

				res[i] = *data;
				""",
				prologue="""IHashTable* RESTRICT table = (IHashTable*)col1[0];
				const size_t next_offset = table->next_offset;
#ifdef __AVX512F__
	if (sizeof(col2[0]) == 8) return avx512_bucket_next(sel, inum, res, (u64*)col2, next_offset);
#endif

				""")
		if types[0] == "u64" and types[1] == "u64":
			gen_primitive(ctx, "bucket_build", result, types,
					"break; (void)res; (void)col1; (void)i;",
				allow_full_eval=False,
				prologue="""
					IHashTable* table = (IHashTable*)col1[0];
					table->create_buckets((IPipeline*)col2[0]);
					""")


	if not is_one_type(types[1]):
		return

	# unary
	types = types[:1]

	if all_prim_types(result, types):
		if is_index(result):
			gen_primitive(ctx, "hash", result, types,
				"""res[i] = voila_hash<{col1_t}>::hash(col1[i]);
	""",
			allow_full_eval=types[0] != "varchar",
			prologue="""
#ifdef __AVX512F__
			return avx512_hash_u32(sel, inum, res, (u32*)col1);
#endif
			""" if (types[0] == "u32" or types[0] == "i32") else "")


		if is_cardinal(result) and is_cardinal(types[0]):
			gen_primitive(ctx, "extract_year", result, types,
				"res[i] = extract_year(col1[i]);")

		if is_cardinal(result) and is_cardinal(types[0]):
			gen_primitive(ctx, "sequence", result, types,
					"res[i] = col1[0] + i;", allow_full_eval=True)

		if result == types[0]:
			gen_primitive(ctx, "broadcast", result, types,
					"res[i] = col1[0];".format(result), allow_full_eval=True)

		if is_cardinal(types[0]) and is_cardinal(result):
			gen_primitive(ctx, "aggr_direct_gsum", result, types,
				"res[0] += col1[i];")
			gen_primitive(ctx, "aggr_direct_gcount", result, types,
				"""(void)i; (void)col1;""",
				prologue="""res[0] += inum; LOG_TRACE("%s: res=%d inum=%d\\n", __func__, res[0], inum);return inum;""")
			gen_primitive(ctx, "aggr_direct_gconst1", result, types,
				"""res[0] = 1; (void)i; (void)col1;""")

		if types[0] == "varchar":
			if is_cardinal(result):
				gen_primitive(ctx, "constant", result, types,
						"res[i] = val_cast;".format(result),
						prologue="const auto val_cast = std::stoll(col1[0].as_string());",
						allow_full_eval=True)
			else:
				gen_primitive(ctx, "constant", result, types,
						"res[i] = col1[0];".format(result),
						allow_full_eval=True)

		if is_sel(result) and is_boolean(types[0]):
			for (name, pre, post) in [("seltrue", "", ""), ("selfalse", "!", "")]:
				gen_primitive(ctx, name, result, types,
					"LOG_TRACE(\"%s: i=%d pred=%d\\n\", __func__, i, " + pre + " col1[i] " + post + "); if (" + pre + " col1[i] " + post + ") {{ res[onum] = i; /*printf(\"%s: i=%lld -> onum=%lld \\n\", __func__, i, onum); */ onum++;}}",
					prologue="""
onum=0;

#ifdef __AVX512F__
return avx512_{name}(sel, inum, res, col1);
#endif
""".format(name=name), epilogue="""
				debug_selection_vector_assert_order((sel_t*)res, onum);

				LOG_TRACE("%s: LWT %d: %s(%p,%lld): returned %d\\n", g_pipeline_name, g_lwt_id,  __func__, col1, inum, (int)onum);
				""", unroll_nosel=8)

		# cast from type to result
		gen_primitive(ctx, "castT" + result, result, types,
				"""res[i] = voila_cast<{col1_t}>::to_{res_t}(col1[i]);

				/*
				if (!i) LOG_DEBUG("%s: col1=%p res=%p\\n", __func__, col1, res);
				LOG_DEBUG("%s: cast %p (with ptr %p) -> %p\\n",
					__func__, col1[i], &col1[i], res[i]);
				*/
				""", allow_full_eval=True)

		if types[0] == "u64":
			gen_primitive(ctx, "bucket_flush", result, types,
				"break; (void)res; (void)col1; (void)i;",
				allow_full_eval=False,
				prologue="""IHashTable* RESTRICT table = (IHashTable*)col1[0]; table->flush2partitions();""")

		if types[0] == result:
			gen_primitive(ctx, "ident", result, types,
				"res[i] = col1[i];", allow_full_eval=True)
			gen_primitive(ctx, "print", result, types,
				"""res[i] = col1[i];
				char* str;
				constexpr size_t buffer_size = voila_cast<{res_t}>::good_buffer_size();
				char buffer[buffer_size];

				const size_t s = voila_cast<{res_t}>::to_cstr(str, &buffer[0], buffer_size, res[i]);
				std::cout << \"col1@\" << i << \"=\" << std::string(str, s) << std::endl;""")


def my_partition(x):
	global num_partitions
	global partition

	return hash(str(x)) % num_partitions == partition

def gen(ctx):
	for r in types:
		for t1 in types:
			for t2 in types:
				for t3 in types:
					partition_set = (r, [t1, t2, t3])
					if my_partition(partition_set):
						for t4 in types:
							for t5 in types:
								for t6 in types:
									ts = [t1, t2, t3, t4, t5, t6]
									wrap(ctx, r, ts)

def main(argv):
	global num_partitions
	global partition

	try:
		opts, args = getopt.getopt(argv,"",["c=","h=","p=","n="])
	except getopt.GetoptError:
		help()
		sys.exit(2)
	for opt, arg in opts:
		if opt in ("--c"):
			c_file = arg
		elif opt in ("--h"):
			h_file = arg
		elif opt in ("--n"):
			num_partitions = int(arg)
		elif opt in ("--p"):
			assert(int(arg) > 0)
			partition = int(arg)-1

	with open(h_file, "w") as h:
		with open(c_file, "w") as c:
			c.write("""
#include "{}"

#include "runtime.hpp"
#include "runtime_struct.hpp"
#include <string>
#include <cstring>
#include <iostream>
""".format(h_file))

			for kernelid in range(0, num_partitions):
				c.write("""#include "kernels{}.hpp"
""".format(kernelid+1))

			h.write("""
#include "runtime.hpp"
#include "runtime_struct.hpp"

""")
			
			gen((h, c))

if __name__ == "__main__":
	main(sys.argv[1:])