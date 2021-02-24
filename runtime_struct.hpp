#ifndef H_RUNTIME_STRUCT
#define H_RUNTIME_STRUCT

#include "runtime.hpp"
#include "runtime_utils.hpp"

#include <atomic>
#include <mutex>
#include <memory>

struct Query;
struct IPipeline;

struct MorselContext : IResetable {
	IPipeline& pipeline;
	size_t index;
	void* last_buffer;

	MorselContext(IPipeline& p) : pipeline(p) {
		reset();
	}

	virtual void reset() final {
		index = 0;
		last_buffer = nullptr;
	}
};

struct Position {
	i64 offset;
	i64 num;

	char* data;

	Position() {
		set_invalid();
	}

	void set_invalid() {
		num = 0;
		offset = 0;
		data = nullptr;
	}

	template<typename T>
	T* get(size_t idx) {
		T* d = (T*)data;
		return &d[idx];
	}

	template<typename T>
	T* current() {
		return get<T>(offset);
	}
};

#define GET_SCAN_POS(Mo, v) Mo.get_scan_pos(v, __func__, __FILE__, __LINE__)
#define GET_READ_POS(Mo, v) Mo.get_read_pos(v, __func__, __FILE__, __LINE__)

#define GET_WRITE_POS(B, num) B.get_write_pos(num, __func__, __FILE__, __LINE__)

#define CAN_GET_POS_SCAN_MORSEL(Mo) Mo.can_get_pos_scan_morsel()
#define CAN_GET_POS_READ_MORSEL(Mo) Mo.can_get_pos_read_morsel()

struct Morsel {
	pos_t _offset = 0;
	pos_t _num = 0;

	pos_t scan_pos = 0;
	pos_t read_pos = 0;

	char* data = nullptr;

	Morsel();

	Position get_scan_pos(pos_t vsize, const char* func, const char* file, int line) {
		return _read_scan_pos(vsize, scan_pos, __func__, func, file, line, true);
	}

	Position get_read_pos(pos_t vsize, const char* func, const char* file, int line) {
		return _read_scan_pos(vsize, read_pos, __func__, func, file, line, false);
	}

	bool _can_get_pos(const pos_t& p, const char* func) const {
		bool r = p < _num;
		// LOG_DEBUG("%s: pos %d, off %d, num %d, data %p -> %d\n", func, p, _offset, _num, data, r);
		return r;
	}

	bool can_get_pos_scan_morsel() const {
		return _can_get_pos(scan_pos, __func__);
	}

	bool can_get_pos_read_morsel() const {
		return _can_get_pos(read_pos, __func__);
	}

	void init(pos_t off = 0, pos_t n = 0, char* d = nullptr) {
		_offset = off;
		_num = n;
		scan_pos = 0;
		read_pos = 0;
		data = d;
	}

private:
	Position _read_scan_pos(pos_t vsize, pos_t& p,
			const char* dbg_name, const char* func, const char* file,
			int line, bool scan) {
		Position r;
		r.offset = p;
		r.data = data;
		if (r.offset >= _num) {
			if (scan) {
				LOG_TRACE("%s: done at %lld num=%lld\n",
					dbg_name, _offset, _num);

			}
			r.set_invalid();
			return r;
		}

		p += vsize;
		r.num = std::min(vsize, _num - r.offset);

		if (scan) {
			LOG_TRACE("%s: %s %s@%d vsize=%lld offsset=%lld num=%lld  morsel_offmset=%lld morsel_num=%lld\n",
				dbg_name, func, file, line, vsize, r.offset, r.num, _offset, _num);
		}

		r.offset += _offset;
		return r;
	}
};

#define IS_VALID(x) _is_valid(x, #x, __FILE__, __LINE__)

inline bool _is_valid(const Position& p, const char* txt, const char* file, const int line) {
	LOG_TRACE("is_valid('%s', Position num=%lld)@%s:%d\n", txt, p.num, file, line);
	return p.num > 0;
}

inline bool _is_valid(pos_t pos, const char* txt, const char* file, const int line) {
	bool r = pos >= 0;
	LOG_TRACE("is_valid('%s' pos_t num=%lld)@%s:%d -> %d\n", txt, pos, file, line, r);
	return r;
}

inline bool _is_valid(const Morsel& m, const char* txt, const char* file, const int line) {
	LOG_TRACE("is_valid('%s' Morsel num=%lld)@%s:%d\n", txt, m._num, file, line);
	return _is_valid(m._num, txt, file, line);
}

struct IBaseColumn {
	void* data;
	size_t size;
	const size_t max_len;
	const int varlen;

	IBaseColumn(Query& q, const std::string& tbl, const std::string& col,
		size_t max_len, int varlen);
};

template<typename T> struct BaseColumn : IBaseColumn {
	T operator[](size_t idx) const {
		return *get(idx);
	}

	T* get(size_t idx) const {
		T* val = (T*)data;
		return &val[idx];
	}


	BaseColumn(Query& q, const std::string& tbl, const std::string& col,
		size_t max_len, int varlen)
	 : IBaseColumn(q, tbl, col, max_len, varlen) {}
};

struct IBaseTable : IResetable {
	Query& query;
	pos_t capacity;

	std::atomic<pos_t> morsel_offset;

public:
	IBaseTable(const char* dbg_name, Query& query);
	virtual void reset() override {
		morsel_offset = 0;
	}

	void get_scan_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file = nullptr, int dbg_line = -1);

	template<typename T>
	void get_scan_morsel(const T& a, MorselContext& ctx,
			const char* dbg_file = nullptr, int dbg_line = -1) {
		get_scan_morsel(a->value, ctx, dbg_file, dbg_line);
	}

	virtual ~IBaseTable();
};



#define GET_SCAN_MORSEL(t, m, c) t->get_scan_morsel(m, c) 
#define GET_READ_MORSEL(t, m, c) t->get_read_morsel(m, c) 

struct ITable;

struct LogicalMasterTable {
	std::mutex mutex;

	std::vector<ITable*> tables;

	void add(ITable& table);

	void get_read_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file = nullptr, int dbg_line = -1);
};

struct BlockFactory;

struct Block {
	size_t num;
	char* data;

	size_t width;
	size_t capacity;

	Block* prev;
	Block* next;

private:
	friend class BlockFactory;

	Block(size_t _width, size_t _capacity, Block* _prev, Block* _next);

public:
	size_t num_free() const {
		return capacity - num;
	}

	size_t size() const {
		return num;
	}

	~Block();
};

struct BlockFactory {
	const size_t width;
	const size_t capacity;

	BlockFactory(size_t width, size_t capacity) noexcept;

	Block* new_block(Block* prev, Block* next);
};

struct BlockedSpace : IResetable {
	Block* head = nullptr;
	Block* tail = nullptr;
	BlockFactory factory;

private:
	Block* new_block_at_end();

public:

	BlockedSpace(size_t width, size_t capacity) noexcept;

	void reset() override;

	Block* append(size_t expected_num) {
		Block* b = tail;
		// fits in tail
		if (!b || b->num_free() < expected_num) {
			b = new_block_at_end();
		}

		ASSERT(b && b->num_free() >= expected_num);
		return b;
	}

	Block* current() {
		return tail;
	}

	size_t size() const;

	template<typename T>
	void for_each(const T& f) {
		Block* b = head;
		while (b) {
			Block* n = b->next;
			f(b);
			b = n;
		}
	}

	template<typename T>
	void for_each(const T& f) const {
		Block* b = head;
		while (b) {
			Block* n = b->next;
			f(b);
			b = n;
		}
	}
private:
	static void partition_data(BlockedSpace** partitions, size_t num_partitions,
		u64* hashes, size_t hash_stride, char* data, size_t width,
		size_t total_num);

public:
	void partition(BlockedSpace** partitions, size_t num_partitions,
		size_t hash_offset, size_t hash_stride) const;


	virtual ~BlockedSpace();
};

struct LargeBuffer;

struct ITable : IResetable {
	const char* dbg_name;
	Query& query;
	const bool m_fully_thread_local;
	const bool m_flush_to_partitions;
	const size_t m_row_width;

	static constexpr size_t m_block_capacity = 2*1024*1024;

	LogicalMasterTable* m_master_table;
	std::vector<BlockedSpace*> m_flush_partitions;

	std::vector<BlockedSpace*> m_write_partitions;

	std::mutex mutex;

	struct ColDef {
		size_t magic = (size_t)(-1);
		size_t offset;
		size_t stride;

		void init(size_t off, size_t str) {
			offset = off;
			stride = str;
		}
	};

	ITable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
		size_t row_width, bool fully_thread_local, bool flush_to_part);

	virtual ~ITable();

	void init();

	virtual void reset_pointers();
	void reset() override;

	struct ThreadView {
		ITable& table;
		BlockedSpace* space;

		ThreadView(ITable& t, IPipeline& pipeline);

		Position get_write_pos(size_t num, const char* func, const char* file, int line) {
			Block* b = space->append(num);

			Position r;

			r.data = b->data;
			r.offset = b->num;
			r.num = num;

			LOG_TRACE("get_write_pos: %s %s@%d data=%p begin=%p end=%p offwset=%lld num=%lld blk->num=%lld\n",
				func, file, line, r.data,
				r.data + (b->num * b->width),
				r.data + ((r.num + b->num) * b->width),
				r.offset, r.num, b->num);

			b->num+=num;
			return r;
		}
	};

private:
	void** hash_index_head = nullptr;
	u64 hash_index_mask = 0;
	LargeBuffer* hash_index_buffer = nullptr;

public:
	size_t hash_index_capacity = 0; //!< #Buckets in 'hash_index_head'
	size_t hash_index_tuple_counter_seq = 0;
	std::atomic<size_t> hash_index_tuple_counter_par;

	size_t hash_stride = 0;
	size_t hash_offset = 0;

	size_t next_stride = 0;
	size_t next_offset = 0;

	void create_hash_index_handle_space(void** buckets, u64 mask, const size_t* thread_id,
		bool parallel, const BlockedSpace* space);

	void create_hash_index(void** buckets, u64 mask, const size_t* thread_id);
	void remove_hash_index();

public:
	void** get_hash_index() const {
		return hash_index_head;
	}

	u64 get_hash_index_mask() const {
		return hash_index_mask;
	}

public:
	bool build_index(bool force, IPipeline* pipeline);

	size_t get_non_flushed_table_count() const {
		size_t c = 0;
		for (const auto& write : m_write_partitions) {
			c += write->size();
		}
		return c;
	}

	void flush2partitions();

	void get_read_morsel(Morsel& morsel, MorselContext& ctx, const char* dbg_file = nullptr, int dbg_line = -1);

	size_t get_row_width() const {
		return m_row_width;
	}

protected:
	typedef u64 SpaceFlags;
	static constexpr SpaceFlags kWriteSpace = 1 << 1;
	static constexpr SpaceFlags kFlushSpace = 1 << 2;
	static constexpr SpaceFlags kAllSpaces = kWriteSpace | kFlushSpace;

	template<typename T>
	void ForEachSpace(const T& f, SpaceFlags = kAllSpaces) {
		for (auto& write_part : m_write_partitions) {
			f(write_part);
		}
		for (auto& flush_part : m_flush_partitions) {
			f(flush_part);
		}
	}
};

struct IHashTable : ITable {
	IHashTable(const char* dbg_name, Query& q, LogicalMasterTable* master_table,
		size_t row_width, bool fully_thread_local, bool flush_to_part);


	template<bool PARALLEL>
	Block* hash_append_prealloc(size_t num) {
		// load factor > 50%?
		const size_t count = PARALLEL ?
			hash_index_tuple_counter_par.load() :
			hash_index_tuple_counter_seq;

		if (UNLIKELY((count + num) * 2 > hash_index_capacity)) {
			build_index(false, nullptr);
		}

		Block* b = m_write_partitions[0]->append(num);
		LOG_TRACE("prealloc:= n=%d\n", num);
		return b;
	}

	template<bool PARALLEL>
	void hash_append_prune(Block* b, size_t n) {
		// proper count
		if (PARALLEL) {
			hash_index_tuple_counter_par+=n;
			ASSERT(false && "untested");
		} else {
			hash_index_tuple_counter_seq+=n;
			LOG_TRACE("prune:= b=%d +  n=%d\n", b->num, n);
			b->num+=n;
		}
	}

	auto scalar_hash_append_prealloc(size_t n) {
		return hash_append_prealloc<false>(n);
	}

	auto scalar_hash_append_prune(Block* b, size_t n) {
		return hash_append_prune<false>(b, n);
	}

	void reset() override;

	void create_buckets(IPipeline* pipeline) {
		build_index(true, pipeline);
	}

	void get_current_block_range(char** begin, char** end,
		size_t num);
};

///// Normalize helpers into function, so we can easily use them from clite:


#define BUFFER_CONTROL_SHALL_REFILL(B) (B).control_shall_refill()
#define BUFFER_CONTROL_SHALL_FLUSH(B) (B).control_shall_flush()
#define BUFFER_CONTROL_OVERFULL_MARGIN(B) (B).control_overfull_margin()
#define BUFFER_SIZE(B) (B).get_buffer_size()

#define BUFFER_DBG_PRINT(B, ...) (B).dbg_print_state(__VA_ARGS__)

#define BUFFER_WRITE_BEGIN(B, ...) (B).write_begin(__VA_ARGS__)
#define BUFFER_WRITE_COMMIT(B, ...) (B).write_commit(__VA_ARGS__)

#define BUFFER_READ_BEGIN(B, ...) (B).read_begin(__VA_ARGS__)
#define BUFFER_READ_COMMIT(B, ...) (B).read_commit(__VA_ARGS__)

#define BUFFER_IS_EMPTY(B) (B).is_empty()

#define BUFFER_CONTEXT_OFFSET(C) C.offset
#define BUFFER_CONTEXT_LENGTH(C) C.length
#define BUFFER_CONTEXT_IS_VALID_INDEX(C, I) C.is_valid_index(I)

#define POSITION_OFFSET(P) P.offset
#define POSITION_NUM(P) P.num


template<typename TABLE, typename INDEX>
u64 __scalar_bucket_lookup(const TABLE* table, INDEX idx, void** HASH_INDEX, u64 HASH_MASK)
{
	const u64 index = idx & HASH_MASK;
	LOG_TRACE("lookup hash=%p mask=%p idx->%d index=%d\n",
		HASH_INDEX, HASH_MASK, idx, index);
	return (u64)(HASH_INDEX[index]);
}

#define SCALAR_BUCKET_LOOKUP(TABLE, INDEX, HASH_INDEX, HASH_MASK) __scalar_bucket_lookup(TABLE, INDEX, HASH_INDEX, HASH_MASK)


#define SCALAR_BUCKET_INSERT(TABLE, INDEX, ROW_TYPE)  \
	__scalar_bucket_insert<ROW_TYPE>(TABLE, INDEX)

template<typename ROW_TYPE, typename TBL_TYPE, typename IDX_TYPE>
u64 __scalar_bucket_insert(TBL_TYPE* table, IDX_TYPE idx)
{
	Block* block = table->scalar_hash_append_prealloc(1);
	char* new_bucket = block->data + (block->width * block->num);
	auto row = (ROW_TYPE*)new_bucket;
	const u64 index = idx & table->get_hash_index_mask();
	void** buckets = table->get_hash_index();
	row->next = (u64)buckets[index];
	buckets[index] = new_bucket;
	table->scalar_hash_append_prune(block, 1);

	LOG_TRACE("insert hash=%p mask=%p idx->%d index=%d -> new_bucket=%p\n",
		buckets, table->get_hash_index_mask(), idx, index, new_bucket);
	return (u64)new_bucket;
}


template<typename S, typename T>
void __scalar_output(S& query, T& val)
{
	size_t buffer_size = voila_cast<T>::good_buffer_size();;
	char* str; char buffer[buffer_size];
	buffer_size = voila_cast<T>::to_cstr(str, &buffer[0], buffer_size, val);
	query.result.push_col(str, buffer_size);
}

#define SCALAR_OUTPUT(v) __scalar_output(query, v)

#define SCALAR_SCAN(TABLE, COL, INDEX) (thread.TABLE->col_##COL[INDEX])
#define DEREFERENCE(p) *(p)
#define ADDRESS_OF(v) &(v)

#define NOT_NULL(x) __not_null(x)
template<typename T>
T __not_null(const T& x)
{
	DBG_ASSERT(x);
	return x;
}

#define ACCESS_BUFFERED_ROW_EX(BUFFER, ROW_TYPE) (BUFFER).current<ROW_TYPE>()
#define ACCESS_BUFFERED_ROW(BUFFER, ROW_TYPE, COL) (BUFFER).current<ROW_TYPE>()->col_##COL
#define ACCESS_ROW_COLUMN(ROW, COL) (NOT_NULL(ROW))->COL

#define AGGR_SUM(COL, X) COL += X;
#define AGGR_COUNT(COL, X) COL++;
#define AGGR_MIN(COL, X) if (COL > X) COL = X;
#define AGGR_MAX(COL, X) if (COL < X) COL = X;

#define SCALAR_AGGREGATE(TYPE, COL, VAL) { auto& col = COL; AGGR_##TYPE(col, VAL); LOG_TRACE("type = %s, val = %d, result = %d\n", #TYPE, VAL, col);}

#include "runtime_simd.hpp"

#ifdef __AVX512F__
template<typename ROW_TYPE, typename TABLE>
inline static void __SIMD_BUCKET_INSERT(_fbuf<u64, 8>& r, TABLE& table, __mmask8 predicate,
	const _fbuf<u64, 8>& indices)
{
	size_t inum = popcount32(predicate);
		_fbuf_set1<u64, 8>(r, 0);

	if (UNLIKELY(!inum)) {
		LOG_TRACE("SIMD_BUCKET_INSERT: inum %d\n", inum);
		return;
	}
	size_t num_inserted = 0;

	/* prealloc space for potential new groups */
	Block* block = table->scalar_hash_append_prealloc(inum);

	/* to later detect conflicts, determine pointer range inside current block */
	u64 ptr_begin = 0;
	u64 ptr_end = 0;
	table->get_current_block_range((char**)&ptr_begin, (char**)&ptr_end, inum);
	/* set common table stuff */
	const u64 bucket_mask = table->get_hash_index_mask();
	u64* heads = (u64*)table->get_hash_index();
	const size_t row_width = table->get_row_width();

#define A(ALL_TRUE, OFFSET) if (ALL_TRUE || ( predicate & (1 << OFFSET))) { \
		const u64 idx = indices.a[OFFSET] & bucket_mask; \
		auto& head = heads[idx]; \
		u64 result = 0; \
		bool conflict = head >= ptr_begin && head <= ptr_end; \
		if (!conflict) { \
			char* new_bucket = ((char*)ptr_begin + (row_width*num_inserted)); \
			auto bucket = (ROW_TYPE*)new_bucket;; \
			result = (u64)bucket; \
			bucket->next = head; \
			head = result; \
			DBG_ASSERT(head); \
			num_inserted++; \
		} \
		r.a[OFFSET] = result; \
		LOG_TRACE("SIMD_BUCKET_INSERT(%d): result = %p\n", OFFSET, result); \
		\
	}

	if (predicate == 0xFF) {
		A(true, 0);
		A(true, 1);
		A(true, 2);
		A(true, 3);
		A(true, 4);
		A(true, 5);
		A(true, 6);
		A(true, 7);
	} else {
		A(false, 0);
		A(false, 1);
		A(false, 2);
		A(false, 3);
		A(false, 4);
		A(false, 5);
		A(false, 6);
		A(false, 7);
	}

#undef A
	LOG_TRACE("inserted %lld\n", num_inserted);
	table->scalar_hash_append_prune(block, num_inserted);
}

template<typename ROW_TYPE, typename TABLE>
inline static void __SIMD_BUCKET_INSERT(_fbuf<u64, 8>& r, TABLE& table, __mmask8 predicate,
	const _v512& indices)
{
	_fbuf<u64, 8> tmp;
	#define A(I) tmp.a[I] = SIMD_REG_GET(indices, u64, I)

	A(0); A(1);
	A(2); A(3);
	A(4); A(5);
	A(6); A(7);

	#undef A
	__SIMD_BUCKET_INSERT<ROW_TYPE, TABLE>(r, table, predicate, tmp);
}

template<typename ROW_TYPE, typename TABLE>
inline static void __SIMD_BUCKET_INSERT(_v512& r, TABLE& table, __mmask8 predicate,
	const _v512& indices)
{
	_fbuf<u64, 8> tmp;
	__SIMD_BUCKET_INSERT<ROW_TYPE, TABLE>(tmp, table, predicate, indices);

	#define A(I) SIMD_REG_GET(r, u64, I) = tmp.a[I];

	A(0); A(1);
	A(2); A(3);
	A(4); A(5);
	A(6); A(7);

	#undef A

}

#define SIMD_BUCKET_INSERT(RESULT, ROW_TYPE, TABLE, PREDICATE, INDICES) \
	__SIMD_BUCKET_INSERT<ROW_TYPE>(RESULT, TABLE, PREDICATE, INDICES);

#include <sstream>

inline static std::ostream&
operator <<(std::ostream &o, const __mmask8& buffer)
{
	std::ostringstream ss;
    ss << "__mmask8(0x" << std::hex << (int)buffer << ")";
    return o << ss.str();
}


inline static std::ostream&
operator <<(std::ostream &o, const __int128_t& buffer)
{
	std::ostringstream ss;
    ss << "i128(" << std::hex << buffer;
    return o << ss.str() << ")";
}

inline static std::ostream&
operator <<(std::ostream &o, const Position& buffer)
{
    return o << "Position (off=" << buffer.offset << ", num=" << buffer.num << ")";
}

inline static std::ostream&
operator <<(std::ostream &o, const varchar& buffer)
{
	std::ostringstream ss;
#if 0
    ss << "varchar (";

    if (buffer.arr) {
    	ss << "'" << buffer.as_string() << "')";
    } else {
    	ss << "NULLPTR)";
    }
#endif
    return o << ss.str();
}


inline static std::ostream&
operator <<(std::ostream &o, const _v512& buffer)
{
	std::ostringstream ss;

	ss << "_v512(";
    for (int i=0; i<8; i++) {
    	if (i) {
    		ss << ", ";
    	}
    	ss << "0x" << std::hex << buffer._i64[i];
    }
    ss << ")";
    return o << ss.str();
}


inline static std::ostream&
operator <<(std::ostream &o, const _v256& buffer)
{
	std::ostringstream ss;

	ss << "_v256(";
    for (int i=0; i<8; i++) {
    	if (i) {
    		ss << ", ";
    	}
    	ss << "0x" << std::hex << buffer._i32[i];
    }
    ss << ")";
    return o << ss.str();
}

inline static std::ostream&
operator <<(std::ostream &o, const _v128& buffer)
{
	std::ostringstream ss;

	ss << "_v128(";
    for (int i=0; i<8; i++) {
    	if (i) {
    		ss << ", ";
    	}
    	ss << "0x" << std::hex << buffer._i16[i];
    }
    ss << ")";
    return o << ss.str();
}


template<typename T, size_t N>
inline static std::ostream&
operator <<(std::ostream &o, const _fbuf<T, N>& buffer)
{
	std::ostringstream ss;

	ss << "_fbuf<" << N << ">(";
    for (int i=0; i<N; i++) {
    	if (i) {
    		ss << ", ";
    	}
    	ss << "0x" << std::hex << buffer.a[i];
    }
    ss << ")";
    return o << ss.str();
}
#endif

#endif
