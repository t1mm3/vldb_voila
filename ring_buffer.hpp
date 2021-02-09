#ifndef H_RING_BUFFER
#define H_RING_BUFFER

#include <string>
#include <cassert>

template<bool CONC_READ = true, bool CONC_WRITE = true>
struct GenericBuffer {
	struct Pos {
		size_t allocated = 0;
		size_t committed = 0;

		void clear() {
			allocated = 0;
			committed = 0;
		}
	};

	Pos m_read;
	Pos m_write;
	size_t m_size;

	const size_t m_capacity;
	const char* dbg_name;

	GenericBuffer(size_t cap, const char* dbg_name)
	 : m_capacity(cap), dbg_name(dbg_name) {
	 	clear();
	}

	void clear() {
		m_read.clear();
		m_write.clear();

		m_size = 0;
	}

	bool empty() const {
		return !m_size;
	}

	size_t size() const {
		return m_size;
	}

	void dbg_enforce_invariants() const {
		assert(m_read.allocated <= m_capacity);
		assert(m_read.committed <= m_capacity);
		assert(m_write.allocated <= m_capacity);
		assert(m_write.committed <= m_capacity);

		assert(m_size <= m_capacity);
	}

	void dbg_print(const char* n) const {
		printf("%s %s size %d\n",
			dbg_name, n, m_size);
		printf("%s %s read: alloc %d, commit %d\n",
			dbg_name, n, m_read.allocated, m_read.committed);
		printf("%s %s write: alloc %d, commit %d\n",
			dbg_name, n, m_write.allocated, m_write.committed);
		printf("\n");
	}

	template<typename T>
	static constexpr T wrap_around(const T& a, const T& num) {
		if (a >= num) {
			return a - num;
		}
		return a;
	}

	size_t write_begin(size_t& offset, size_t num) {
		dbg_enforce_invariants();
		offset = m_write.allocated;

		size_t free = m_capacity - m_size;
		free = std::min(free, m_capacity - m_write.allocated);
		num = std::min(free, num);

		m_write.allocated = wrap_around(m_write.allocated + num, m_capacity);

		dbg_enforce_invariants();
		return num;
	}

	void write_end(size_t num) {
		dbg_enforce_invariants();

		m_write.committed = wrap_around(m_write.committed + num, m_capacity);
		m_size += num;

		// dbg_print("write_end");

		dbg_enforce_invariants();
	}


	size_t read_begin(size_t& offset, size_t num) {
		dbg_enforce_invariants();

		dbg_print("read_begin");

		offset = m_read.allocated;

		size_t readable = m_size;
		readable = std::min(readable, m_capacity - m_read.allocated);
		num = std::min(readable, num);

		m_read.allocated = wrap_around(m_read.allocated + num, m_capacity);

		dbg_enforce_invariants();
		return num;	
	}

	void read_end(size_t num) {

		dbg_print("read_end");
		dbg_enforce_invariants();
		m_read.committed = wrap_around(m_read.committed + num, m_capacity);
		m_size -= num;
		dbg_enforce_invariants();
	}
};

#endif