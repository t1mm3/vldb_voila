#ifndef H_RUNTIME_BUFFERING
#define H_RUNTIME_BUFFERING

#include "runtime_utils.hpp"
#include "runtime_framework.hpp"
#include "runtime.hpp"

#define BUFFER_ASSERT(x) ASSERT(x)

#ifdef TEST_BUFFERING
extern void reset_watermark(size_t x);
extern void set_watermark(size_t x);
#endif

// #define BUFFER_STATS


// #define DBG_PADDING 1024
template<size_t MAX_FILL, size_t MIN_FILL,
	size_t READ_GRAN, size_t WRITE_GRAN,
	size_t BUFFER_SIZE>
struct SimpleBuf : IFujiObject {
private:
#ifdef DBG_PADDING
	size_t dbg_pad_pre[DBG_PADDING];
#endif

	size_t count;
	size_t read_index;
	size_t write_index;
	size_t watermark; //!< Area size. Sometimes at the end will try to write bigger chunks.


#ifdef BUFFER_STATS
	size_t inflow, outflow, num_wwraps, num_rwraps;
#endif

#ifdef DBG_PADDING
	size_t dbg_pad_post[DBG_PADDING];
#endif

public:
	static constexpr size_t max_write = MAX_FILL;
	static constexpr size_t min_write = MIN_FILL;
	static constexpr size_t size = BUFFER_SIZE;
	static constexpr size_t reserved = size;

	static constexpr size_t invalid_watermark = reserved;

	static constexpr bool is_valid_index(size_t idx) {
		return idx <= reserved;
	}

	SimpleBuf() {
		read_index = 0;
		write_index = 0;
		count = 0;
		watermark = invalid_watermark;
#ifdef BUFFER_STATS
		inflow = 0;
		outflow = 0;
		num_rwraps = 0;	
		num_wwraps = 0;	
#endif
#ifdef DBG_PADDING
		for (int i=0; i<DBG_PADDING; i++) {
			dbg_pad_post[i] = i;
			dbg_pad_pre[i] = i;
		}
#endif
	}

	void print_stats(std::ostream& o) override {
		IFujiObject::print_stats(o);
#ifdef BUFFER_STATS
		o 	<< dbg_name << ":"
			<< "inflow " << inflow << ", outflow " << outflow << ", "
			<< "write_wraps " << num_wwraps << ", read_wraps " << num_rwraps
			<< std::endl; 
#endif
	}

	void check_dbg_padding() const {
#ifdef DBG_PADDING
		for (int i=0; i<DBG_PADDING; i++) {
			ASSERT(dbg_pad_post[i] == i);
			ASSERT(dbg_pad_pre[i] == i);
		}	
#endif
	}

	void check_invariants() const {
#if 0
		if (write_index == read_index) {
			BUFFER_ASSERT(count == 0 || count == reserved);
		} else {
			size_t c;
			if (write_index > read_index) {
				c = write_index - read_index;
			} else {
				c = watermark - read_index + write_index;
			}
			BUFFER_ASSERT(count == c);
		}
#endif
	}

	struct ReadContext {
		size_t offset;
		size_t length;
	};


	void read_begin(ReadContext& ctx, size_t num) {
		check_dbg_padding();

		check_invariants();

		BUFFER_ASSERT(!is_empty());
		BUFFER_ASSERT(watermark <= reserved);
		BUFFER_ASSERT(count > 0 && count <= size);

		ctx.offset = read_index;

		size_t num_readable;
		if (LIKELY(write_index > read_index)) {
			num_readable = write_index - read_index;
		} else {
			if (LIKELY(write_index != read_index)) {
				num_readable = watermark - read_index;
				if (!num_readable) {
					num_readable = write_index;
					ctx.offset = 0;
				}
			} else {
				num_readable = count;
				if (count) {
					num_readable = std::min(num_readable, watermark - read_index);
				}
			}
		}

		BUFFER_ASSERT(num_readable <= count);

		ctx.length = std::min(num, num_readable);

		BUFFER_ASSERT(ctx.offset + ctx.length <= watermark);

		LOG_TRACE("read_begin: ReadContext offset %d, length %d; num_readable %d, num %d\n",
			(int)ctx.offset, (int)ctx.length, (int)num_readable, (int)num);

		BUFFER_ASSERT(ctx.length);
	}

	void read_commit(ReadContext& ctx) {
		check_dbg_padding();

		LOG_TRACE("read_commit: ReadContext offset %d, length %d\n",
			(int)ctx.offset, (int)ctx.length);

		BUFFER_ASSERT(!is_empty());

		size_t next = ctx.offset + ctx.length;
		if (next >= watermark) {
			LOG_DEBUG("read_commit(%p): RESET read %d write %d reserved %d count %d watermark %d\n",
				this, (int)read_index, (int)write_index, (int)reserved, (int)get_buffer_size(),
				(int)watermark);

			BUFFER_ASSERT(watermark <= reserved);
			BUFFER_ASSERT(next == watermark);
			next = 0;

#ifdef BUFFER_STATS
			num_rwraps++;
#endif
			watermark = invalid_watermark;
#ifdef TEST_BUFFERING
			reset_watermark(invalid_watermark);
#endif
		}

		read_index = next;

#ifdef BUFFER_STATS
		outflow += ctx.length;
#endif

		count -= ctx.length;
		BUFFER_ASSERT(count >= 0 && count <= size);

		check_invariants();
	}




	struct WriteContext {
		size_t offset;
		size_t length;
	};

	void write_begin(WriteContext& ctx, size_t num) {
		check_invariants();
		check_dbg_padding();

		BUFFER_ASSERT(count >= 0 && count <= size);
		BUFFER_ASSERT(count + num <= size && "Must fit");

		LOG_TRACE("write_begin(%p): read %d write %d num %d reserved %d count %d  watermark %d\n",
			this, (int)read_index, (int)write_index, (int)num, (int)reserved,
			(int)get_buffer_size(), (int)watermark);

		if (write_index + num > reserved) {
			// would overflow
			// we remember the position but
			// and wrap around, so that we
			// can use a single write
			BUFFER_ASSERT(watermark == invalid_watermark);
			watermark = write_index;
#ifdef TEST_BUFFERING
			set_watermark(write_index);
#endif


#ifdef BUFFER_STATS
			num_wwraps ++;
#endif
			write_index = 0;
			dbg_print_state("post set");

			// sanity check
			BUFFER_ASSERT(watermark <= reserved);
			if (get_buffer_size())
				BUFFER_ASSERT(write_index + num < read_index);

			LOG_TRACE("write_begin(%p): watermark at %d\n",
				this, (int)watermark);
		}

		ctx.offset = write_index;
		ctx.length = num;
		BUFFER_ASSERT(write_index + num <= reserved);

		LOG_TRACE("write_begin: WriteContext offset %d, length %d\n",
			(int)ctx.offset, (int)ctx.length);
	}

	void write_commit(WriteContext& ctx) {
		check_dbg_padding();

		LOG_TRACE("write_commit: WriteContext offset %d, length %d\n",
			(int)ctx.offset, (int)ctx.length);

		size_t next = ctx.offset + ctx.length;

		if (next >= reserved) {
			BUFFER_ASSERT(next == reserved);
			LOG_TRACE("write_commit(%p): RESET\n", this);
			next = 0;
			watermark = invalid_watermark;
		}
		// if (next == read_index) { BUFFER_ASSERT(false && "Buffer full"); }

		write_index = next;


#ifdef BUFFER_STATS
		inflow += ctx.length;
#endif
		count += ctx.length;
		BUFFER_ASSERT(count > 0 && count <= size);
		check_invariants();
	}

	size_t get_buffer_size() const {
		return count;
	}

	size_t get_max_buffer_size() const {
		return size;
	}

	bool is_empty() const {
		return !count;
	}

	void dbg_print_state(const char* pre = "", const char* post = "\n") {
		LOG_TRACE("%s: LWT %d: %s (%p) count %d, read %d, write %d, watermark %d, loss %d%s",
			g_pipeline_name, g_lwt_id,
			pre, this, (int)get_buffer_size(), (int)read_index, (int)write_index,
			(int)watermark, (int)(reserved - watermark), post);
	}


	bool control_shall_flush() const {
		bool r = get_buffer_size() >= MAX_FILL;
		if (r) {
			LOG_TRACE("%s: %s: flush\n", g_pipeline_name, dbg_name);
		}
		return r;
	}

	size_t control_overfull_margin() const {
		bool r = get_buffer_size() >= MAX_FILL;

		if (!r)	{
			return 0;
		}

		return MAX_FILL - get_buffer_size();
	}

	bool control_shall_refill() const {
		static_assert(MIN_FILL == 0, "Low watermark is not yet supported");
		bool r = is_empty();
		if (r) {
			LOG_TRACE("%s: %s: refill\n", g_pipeline_name, dbg_name);
		}
		return r;
	}
};

template<typename T, size_t SIZE>
struct StaticArray {
	T* get() {
		return m_data;
	}

	StaticArray() {
		m_data = new T[SIZE]; 
	}

	~StaticArray() {
		delete[] m_data;
	}

private:
	T* m_data;
};

#endif