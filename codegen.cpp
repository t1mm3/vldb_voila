#include "codegen.hpp"
#include "voila.hpp"
#include "utils.hpp"
#include "common/runtime/Import.hpp"
#include "runtime_framework.hpp"
#include "printing_pass.hpp"
#include "codegen_passes.hpp"
#include <sstream> 
#include "common/runtime/Types.hpp"

CgBaseCol::CgBaseCol(const std::string& source, runtime::Attribute& a)
 : rt_ref(a), source(source)
{
	type = a.type->to_voila();
	dmin = a.minmax->lo;
	dmax = a.minmax->hi;
	varlen = a.minmax->flags & MinMaxInfo::kVariableSize;
	maxlen = a.minmax->max_len;
}

Codegen::Codegen(QueryConfig& config)
 : config(config)
{

}


static size_t
size_of_type(const std::string& t)
{
#define TYPE(TYPE, _) if (!t.compare(#TYPE)) { \
		return sizeof(TYPE); \
	}

	TYPE_EXPAND_ALL_TYPES(TYPE, 0)
#undef TYPE

	return 0;
}

inline std::string
bool2str(bool b)
{
	return std::string(b ? "true" : "false");
}

void
Codegen::gen_table(std::ostringstream& out, DataStructure& d,
	const std::string& id, DataStructure::Type t,
	const std::vector<TableColumn>& cols)
{
	const std::string t_hash("u64");
	const std::string t_next("u64");

	std::string hash_col;

	std::string base = "ITable";
	if (t == DataStructure::Type::kHashTable) {
		base = "IHashTable";
	}

	const auto type = "__struct_" + id;

	out << "struct " << type << " : " << base << " {" << std::endl;

	// NSM row
	out<< "struct Row {";

	size_t max_type = 0;
	size_t size = 0;

	auto add_attr = [&] (const auto& type) {
		size_t sz = size_of_type(type);
		ASSERT(sz > 0);
		size += sz;
		max_type = std::max(max_type, sz);
	};

	map_keys_first(cols, [&] (auto c, auto is_key) {
		(void)is_key;
		const std::string colname("col_"+ c.name);
		ASSERT(c.name.size() > 0);
		out << c.type << " " << colname << ";";
		if (is_key) {
			out << "/* key */";
		} else {
			out << "/* value */";
		}
		out << std::endl;

		if (c.hash) {
			ASSERT(!hash_col.size() && "Only one hash per table is allowed");
			ASSERT(!t_hash.compare(c.type));
			hash_col = colname;
		}

		add_attr(c.type);
	});

	if (t == DataStructure::Type::kHashTable) {
		if (!hash_col.size()) {
			out << t_hash << " hash;" << std::endl;
			add_attr(t_hash);
		}
		out << t_next << " next;" << std::endl;
		add_attr(t_next);
	}

	ASSERT(size > 0);

	size_t padding = size % max_type;
	for (size_t i=0; i<padding; i++) {
		out << "u8 pad" << i << ";" << std::endl;
	}

	out << "}; /* Row */" << std::endl
		<< "Row* rows = nullptr;" << std::endl;

	// NSM->DSM row
	map_keys_first(cols, [&] (auto c, auto is_key) {
		(void)is_key;
		out << "TableVec<"<< c.type << "> col_" <<  c.name << ";" << std::endl
			<< "size_t stride_" <<  c.name << ";" << std::endl
			<< "size_t offset_" <<  c.name << ";" << std::endl
			<< "ITable::ColDef coldef_" << c.name << ";" << std::endl;
	});

	out << type << "(Query& q, LogicalMasterTable* master_table, "
		<< "size_t num_rows) : " << base << "(\"" << id << "\", q, master_table, sizeof(Row), "
		<< bool2str(d.flags & DataStructure::kThreadLocal) << ", "
		<< bool2str(d.flags & DataStructure::kFlushToMaster)
		<< ")";

	map_keys_first(cols, [&] (auto c, auto is_key) {
		(void)is_key;
		out << ", col_" << c.name << "(\"" << c.name << "\", q)";
	});

	out << " { init(); }" << std::endl;
	out << "void reset_pointers() override {" << std::endl;
	// out << "rows = (Row*)table;" << std::endl;
	out << "Row dummy_row;" << std::endl;

	map_keys_first(cols, [&] (auto c, auto is_key) {
		(void)is_key;
		ASSERT(c.name.size() > 0);
		out
			// << "col_" << c.name << ".reset_pointer(&rows[0].col_" << c.name << ");"
			<< "stride_" << c.name << " = sizeof(Row) / sizeof(" << c.type << ");" << std::endl
			<< "offset_" << c.name << " = " << "(char*)&dummy_row.col_" << c.name << " - (char*)&dummy_row;" << std::endl
			<< "static_assert(sizeof(Row) % sizeof(" << c.type << ") == 0, \"Must be a multiple\");" << std::endl
			<< "col_" << c.name << ".reset_pointer(offset_" << c.name << ");" << std::endl
			<< "coldef_" << c.name << ".init(offset_" << c.name << ", stride_" << c.name << ");" << std::endl;

#if 0
		// test
		out << "{ // quick test" << std::endl
			<< "auto vcol = [&] (auto idx) { return &col_"<< c.name << ".get()[idx*stride_" << c.name << "];};" << std::endl
			<< "auto rcol = [&] (auto idx) { return &rows[idx].col_" << c.name << "; };" << std::endl
			<< "auto v1 = vcol(0); auto v2 = vcol(13); auto r1 = vcol(0); auto r2 = vcol(13);" << std::endl
			<< "ASSERT(r1 == v1);"  << std::endl
			<< "ASSERT(r2 == v2);"  << std::endl
			<< "}" << std::endl;
#endif
	});

	if (t == DataStructure::Type::kHashTable) {
		if (!hash_col.size()) {
			hash_col = "hash";
		}

		out
			<< "hash_offset" << " = " << "(char*)&dummy_row." << hash_col << " - (char*)&dummy_row;" << std::endl
			<< "hash_stride" << " = sizeof(Row) / sizeof(" << t_hash << ");" << std::endl
			<< "static_assert(sizeof(Row) % sizeof(" << t_hash << ") == 0, \"Must be a multiple\");" << std::endl;

		out << "next_offset = " << "(char*)&dummy_row.next - (char*)&dummy_row;" << std::endl
			<< "next_stride = sizeof(Row) / sizeof(" << t_next << ");" << std::endl
			<< "static_assert(sizeof(Row) % sizeof(" << t_next << ") == 0, \"Must be a multiple\");" << std::endl;


#if 0
		out << "arr_hash = &rows[0]." << hash_col << "; stride_hash = sizeof(Row) / sizeof(" << t_hash << ");" << std::endl
			<< "static_assert(sizeof(Row) % sizeof(" << t_hash << ") == 0, \"Must be a multiple\");" << std::endl;
		out << "arr_next = &rows[0].next; stride_next = sizeof(Row) / sizeof(" << t_next << ");" << std::endl
			<< "static_assert(sizeof(Row) % sizeof(" << t_next << ") == 0, \"Must be a multiple\");" << std::endl;
		out << "hashs = arr_hash; hash_stride = stride_hash;" << std::endl
			<< "nexts = arr_next; next_stride = stride_next;" << std::endl;
#endif
	}

	out << "}" << std::endl;

	out << "}; /* " << id << "*/" << std::endl;
}

void
Codegen::gen_lolepop(Lolepop& l, Pipeline& p)
{
	(void)l;
	(void)p;
}

void
Codegen::gen_pipeline(Pipeline& p, size_t number)
{
	(void)p;
	(void)number;
}

std::string
Codegen::operator()(Program& p)
{
	decl << "#include \"runtime.hpp\"" << std::endl
		 << "#include \"runtime_vector.hpp\"" << std::endl
		 << "#include \"runtime_struct.hpp\"" << std::endl
		 << "#include \"runtime_hyper.hpp\"" << std::endl
		 << "#include \"runtime_simd.hpp\"" << std::endl
		 << "#include \"runtime_translation.hpp\"" << std::endl
		 << "#include \"runtime_framework.hpp\"" << std::endl
		 << "#include \"runtime_struct.hpp\"" << std::endl
		 << "#include \"runtime_buffering.hpp\"" << std::endl;

	for (int i=1; i<=32; i++) {
		decl << "#include \"kernels" << i << ".hpp\"" << std::endl;		
	}

	decl << "#include <string>" << std::endl
		 << "#include <iostream>" << std::endl
		 << "#include <memory>" << std::endl;

		// create base tables
	for (auto& d : p.data_structures) {
		if (d.type != DataStructure::Type::kBaseTable) {
			continue;
		}
		gen_datastructure(d, p);
	}

	CodegenPassPipeline passes(*this, config);
	passes(p);

	PrintingPass printer;
	printer(p);

	decl << "#if 0" << std::endl
		 << printer.str() << std::endl
		 << "#endif" << std::endl;


	for (auto& d : p.data_structures) {
		if (d.type == DataStructure::Type::kBaseTable) {
			continue;
		}
		gen_datastructure(d, p);
	}

	size_t num = p.pipelines.size();
	for (size_t i=0; i<num; i++) {
		auto& pl = p.pipelines[i];
		last_pipeline = i+1 == num;
		gen_pipeline(pl, i);
	}

	std::ostringstream out;

	out << "// decl" << std::endl;
	out << decl.str() << std::endl;

	{
		std::ostringstream query_decl;
		std::ostringstream query_ctor;
		std::ostringstream query_dtor;

		std::ostringstream thread_decl;
		std::ostringstream thread_ctor;
		std::ostringstream thread_dtor;

		query_ctor << "  ThisQuery(QueryConfig& cfg) : Query(cfg) {" << std::endl;
		query_dtor << "  ~ThisQuery() {" << std::endl;
		thread_ctor << "  ThisThreadLocal(ThisQuery& q) : IThreadLocal(q) {" << std::endl;
		thread_dtor << "  ~ThisThreadLocal() {" << std::endl;

		for (auto& d : p.data_structures) {
			const bool local = d.flags & DataStructure::kThreadLocal;
			const bool base_table = d.type == DataStructure::Type::kBaseTable;

			const bool flush_to_master = local && (d.flags & DataStructure::kFlushToMaster);

			if (base_table) {
				ASSERT(!local);
			}

			const std::string default_struct_args(local ? "64*1024" : "16*1024*1024");
			const std::string arguments(base_table ? "*this" : ("*this, " + default_struct_args));
			const std::string type_prefix(base_table ? "__basetable_" : "__struct_");
			const std::string type(type_prefix + d.name);

			bool dtor = false;
			if (local) {
				if (flush_to_master) {
					query_decl << "  LogicalMasterTable* master_" << d.name << ";"  << std::endl;
					query_ctor << "master_" << d.name << " = new LogicalMasterTable();" << std::endl;	
					query_dtor << "delete master_" << d.name << ";" << std::endl;
				}
			} else {
				query_decl << "  " << type << "* " << d.name << ";"  << std::endl;

				query_ctor << d.name << " = new "<<  "  " << type << " (*this";
				if (!base_table) {
					query_ctor << ", " << "nullptr, " << default_struct_args;
				}
				query_ctor << ")" << ";" << std::endl;	
				dtor = true;
			}

			if (dtor) {
				query_dtor << "delete " << d.name << ";" << std::endl;
			}


			std::string thread_local_type(type);
			if (!local && !base_table) {
				// thread_local_type = thread_local_type + "::ThreadView";
			}


			thread_decl << "  " << thread_local_type << "* " << d.name << ";"  << std::endl;
			if (local) {
				const std::string master(flush_to_master ? "q.master_" + d.name : "nullptr");

				thread_ctor << d.name << " = new "<<  "  " << thread_local_type
					<< " (q, " << master << ", " << default_struct_args << ")" << ";" << std::endl;
				thread_dtor << "delete " << d.name << ";" << std::endl;
			} else {
				//if (base_table) {
					thread_ctor << d.name << " = q." << d.name << ";" << std::endl;	
				//} else {
				//	thread_ctor << d.name << " = new " << thread_local_type << "(q." << d.name
				//		 << ");" << std::endl;	
				//}
			}
		}

		query_ctor << "  }" << std::endl;
		query_dtor << "  }" << std::endl;
		thread_ctor << "  }" << std::endl;
		thread_dtor << "  }" << std::endl;

		out << "struct ThisQuery : Query {" << std::endl
			<< query_decl.str()
			<< query_ctor.str()
			<< query_dtor.str()
			<< "};" << std::endl;

		out << "struct ThisThreadLocal : IThreadLocal {" << std::endl
			<< thread_decl.str()
			<< thread_ctor.str()
			<< thread_dtor.str()
			<< "};" << std::endl;
	}

	out << "// init" << std::endl;
	out << init.str() << std::endl;
	out << "// impl" << std::endl;
	out << impl.str() << std::endl;
	out << "// next" << std::endl;
	out << next.str() << std::endl;
	out << "// main" << std::endl;

	out << "EXPORT Query* voila_query_new(QueryConfig* cfg) {" << std::endl
		<< "  ASSERT(cfg);" << std::endl
		<< "  ThisQuery* q = new ThisQuery(*cfg);" << std::endl
		<< "  ASSERT(q);" << std::endl
		<< "  q->init([&] (auto this_thread, auto num_threads) {" << std::endl
		<< "    return std::vector<IPipeline*>({" << std::endl;

	// list pipelines
	for (size_t i=0; i<p.pipelines.size(); i++) {
		if (i > 0) {
			out << ", ";
		}
		out << "      new Pipeline_" << i << "(*q, this_thread)" << std::endl;
	}

	out << "    });" << std::endl
		<< "  }, [&] (auto this_thread, auto num_threads) { return new ThisThreadLocal(*q); });" << std::endl
		<< "  return q;" << std::endl
		<< "}"<< std::endl
		<< "EXPORT void voila_query_run(ThisQuery* q) { ASSERT(q); q->run(0); }"<< std::endl
		<< "EXPORT void voila_query_reset(ThisQuery* q) { ASSERT(q); q->reset(); }"<< std::endl
		<< "EXPORT void voila_query_free(ThisQuery* q) { ASSERT(q) delete q; }"<< std::endl;

	return out.str();
}

std::string
Codegen::expr2get0(Expression* e, bool ignore_error) 
{
	auto it = expr2var.find(e);
	if (ignore_error) {
		if (it == expr2var.end()) {
			return "";
		}
	} else {
		ASSERT(it != expr2var.end());
	}

	auto& vec = it->second;

	ASSERT(vec.size() == 1);
	ASSERT(vec[0].size() > 0);
	return vec[0];
}

void
Codegen::expr2set0(Expression* e, const std::string& x)
{
	auto it = expr2var.find(e);
	ASSERT(it == expr2var.end());
	expr2var[e] = {x};
}

#include <algorithm>

void
Codegen::gen_base_table(std::ostringstream& out, const std::string& id,
	CgBaseTable& t)
{
	out << "struct __basetable_" << id << " : IBaseTable {" << std::endl;
	// declare columns
	for (auto& c : t.cols) {
		const auto& col = *c.second;
		out << "  " << "BaseColumn<" << col.type << "> col_" << c.first << ";" << std::endl; 
	}

	// loading columns
	out << "__basetable_" << id << "(Query& query) : IBaseTable(\"" << id << "\", query)" << std::endl;
	for (auto& c : t.cols) {
		const auto& col = *c.second;
		out << ",  " << "col_" << c.first << "(query, \"" << t.source << "\", "
			<< "\"" << col.source << "\","
			<< "" << col.maxlen << ", " << col.varlen
			<< ")"<< std::endl; 
	}
	out << "{" << std::endl;
	for (auto& c : t.cols) {
		out << "  capacity = " << "col_" << c.first << ".size;" << std::endl
			<< "  ASSERT(col_" << c.first << ".size > 0);" << std::endl;
	}
	out << "}" << std::endl;
	out << "}; /*" << id << "*/" << std::endl;
}

inline std::string split_colname(const std::string& n)
{
	auto v = split(n, '.');
	return v[1];
}

inline std::string colname(const std::string& t, const std::string& c)
{
	return t + "." + c;
}

void
Codegen::gen_datastructure(DataStructure& d, Program& p)
{
	std::vector<std::string> col_refs;

	for (auto& c : d.cols) {
		col_refs.emplace_back(colname(d.name, c.name));
		LOG_TRACE("finder find '%s'\n", colname(d.name, c.name).c_str());
	}


	FindRefs finder(col_refs);
	finder(p);

	if (d.type == DataStructure::Type::kBaseTable) {
		runtime::Database& b = config.db;
		std::vector<TableColumn> cols;
		auto t = std::make_shared<CgBaseTable>(d.source, b[d.source]);

		// add columns
		for (auto& c : d.cols) {
#if 0
			auto expr = finder.references[d.name + "." + c.name];
			if (!expr) {
				decl << "/* Unused column '" << c.name << "' */" << std::endl;
				continue;
			}
#endif
			t->cols[c.name] = std::make_shared<CgBaseCol>(c.source, t->rt_ref[c.source]);
		}

		base_tables[d.name] = t;
		gen_base_table(decl, d.name, *t);
		return;
	}
	

	// ASSERT(finder.found_all());

	std::vector<TableColumn> cols;
	for (auto& c : d.cols) {
		auto expr = finder.references[colname(d.name, c.name)];
		if (!expr) {
			decl << "/* Unused column '" << c.name << "' */" << std::endl;
			continue;
		}
		ASSERT(expr->props.type.arity.size() == 1);
		auto& type = expr->props.type.arity[0].type;

		cols.emplace_back(TableColumn {type, c.name, c.source,
			c.mod == DCol::Modifier::kKey,
			c.mod == DCol::Modifier::kHash}
		);
	}

	switch (d.type) {
	case DataStructure::Type::kTable:
	case DataStructure::Type::kHashTable:
		gen_table(decl, d, d.name, d.type, cols);
		break;

	case DataStructure::Type::kBaseTable:
		ASSERT(false && "unreachable");
		break;

	default:
		ASSERT(false && "Invalid data structure type");
		break;
	}
	
}

std::string
Codegen::gen_get_pos(const std::string& dest, const std::string& name,
	const std::string& offset_var, const std::string& table,
	const std::string& size)
{
	std::ostringstream s;

	s << dest << " = " << table << ".get_" << name << "("
		<< offset_var << ", " << size <<");" << std::endl;

	return s.str();
}