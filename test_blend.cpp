int A(int x) {
	return x;
}
int B(int x) {
	return x;
}
int C(int x) {
	return x-1;
}

static bool trace = false;
#include <cassert>
#include <stdio.h>
#include <string>
#include <vector>

bool get_cond(int x) {
	return x % 3 == 0;
}

struct BaseTest {
	int r_onum;
	int r_num_inner;

	const std::string m_name;

	BaseTest(const std::string& name) : m_name(name) {

	}

	void print(const std::string& name) {
		printf("%s onum %d num_inner %d\n", name.c_str(), r_onum, r_num_inner);
	}

	void operator()(int* xs, int num) {
		r_onum = 0;
		r_num_inner = 0;


		test(xs, num);
		print(m_name);

		const auto test_onum = r_onum;
		const auto test_inner = r_num_inner;
		r_onum = 0;
		r_num_inner = 0;

		base_case(xs, num);
		print("base");

		assert(r_onum == test_onum);
		assert(r_num_inner == test_inner);
	}

	virtual void test(int* xs, int num) {
		base_case(xs, num);
	}

	void base_case(int* xs, int num) {
		int read_xs = 0;
		int x, a, b;
	 	bool cond;

	  entry:
	 	if (read_xs >= num) return;
	  	x = xs[read_xs++];

	  	goto loop;

	  loop:
	  	//printf("entry %d\n", x);
	  	cond = !get_cond(x);
	  	if (!cond) {
	  		goto post;
	  	}
	  	a = A(x);
	  	b = B(a);
	  	x = C(b);

	  	//printf("loop c=%d\n", x);
	  	r_num_inner++;
	  	goto loop;

	  post:
	  	r_onum++;
	  	goto entry;
	}
};

struct TestStack : BaseTest{

	int b1_size;
	int b2_size;

	int *b1_col_a;
	int b1_write = 0;

	int *b2_col_b;
	int b2_write = 0;

	TestStack(int _b1_size, int _b2_size) : BaseTest("Stack b1=" + std::to_string(_b1_size) + ",b2=" + std::to_string(_b2_size)) {
		b1_size = _b1_size;
		b2_size = _b2_size;

		b1_col_a = new int[b1_size];
		b2_col_b = new int[b2_size];
	}

	~TestStack() {
		delete[] b1_col_a;
		delete[] b2_col_b;
	}

	void test(int* xs, int num) override {
		b1_write = 0;
		b2_write = 0;

		int read_xs = 0;
		int x, a, b;
	 	bool cond;

	 	auto print_state = [&] (const auto& n) {
	 		printf("%s state: b1_write %d b2_write %d\n",
	 			n, b1_write, b2_write);
	 	};

	  entry:
	 	if (read_xs >= num) {
	 		if (trace) print_state("exit");

	 		if (b1_write) goto b1_flush;
	 		if (b2_write) goto b2_flush;

	 		return;
	 	}

	 	if (trace) printf("scan offset=%d\n", read_xs);
	  	x = xs[read_xs++];

	  	goto loop;

	  loop:
	  	if (trace) printf("entry %d\n", x);
	  	cond = !get_cond(x);
	  	if (!cond) {
	  		goto post;
	  	}
	  	a = A(x);
	  	if (trace) printf("b1_write %d\n", b1_write);
	  	assert(b1_write < b1_size);
	  	b1_col_a[b1_write] = a;

	  	b1_write++;

	  	// TRICK: buffer before doing anything else!
	  	// Otherwise b1_flush -> b2_flush -> loop might ge triggered and state overwritten

	  	if (b1_write >= b1_size) {
	  		goto b1_flush;
	  	}
  		// back-edge
  		goto entry;

	  b1_flush:
	  	if (!b1_write) {
	  		goto entry;
	  	}
	  	a = b1_col_a[b1_write-1];
	  	b1_write--;

	  	b = B(a);
		if (trace) printf("b2_write %d\n", b2_write);
		assert(b2_write < b2_size);
		b2_col_b[b2_write] = b;		
		b2_write++;

		if (b2_write >= b2_size) {
			goto b2_flush;
		}
		goto b1_flush;


	  b2_flush:
	  	if (!b2_write) {
	  		goto b1_flush;
	  	}
	  	b = b2_col_b[b2_write-1];
	  	b2_write--;
	  	x = C(b);

	  	if (trace) printf("loop c=%d\n", x);
	  	r_num_inner++;
	  	goto loop;

	  post:
	  	r_onum++;
	  	goto entry;
	}
};

struct CircularBuffer {
	const size_t m_reserved;
	const size_t m_capacity;

	size_t m_head;
	size_t m_tail;

	CircularBuffer(size_t N, size_t StartIndex = 0) : m_reserved(N+1), m_capacity(N) {
		m_head = StartIndex;
		m_tail = StartIndex;
	}

	size_t push(size_t& out_offset) {
		printf("push: head %d, tail %d\n", m_head, m_tail);
		out_offset = m_head;
		size_t next = m_head+1;
		if (next == m_reserved) next = 0;
		if (next == m_tail) return 0;

		m_head = next;
		return 1;
	}

	size_t pop(size_t& out_offset) {
		printf("pop: head %d, tail %d\n", m_head, m_tail);
		if (empty()) return 0;

		out_offset = m_tail;
		size_t next = m_tail + 1;
		if (next == m_reserved) next = 0;

		m_tail = next;
		return 1;
	}

	size_t push(size_t& out_offset, size_t num) {
		size_t num_seq;
		if (m_tail <= m_head) {
			// until 'm_capacity'
			num_seq = m_capacity - m_head;
		} else {
			// until before 'm_tail'
			num_seq = m_tail - m_head - 1;
		}


		printf("pushN: head %d, tail %d, capacity %d, num_seq %d\n",
			m_head, m_tail, m_capacity, num_seq);

		if (num > num_seq) num = num_seq;
		if (!num) return 0;

		out_offset = m_head;
		size_t next = m_head + num;

		if (m_head == m_reserved) next = 0;
		m_head = next;

		return num;
	}

	size_t pop(size_t& out_offset, size_t num) {
		size_t s = size();
		if (num > s) num = s;

		out_offset = m_tail;

		return num;
	}

	size_t size() const {
		printf("size: head %d, tail %d\n", m_head, m_tail);
		return m_head - m_tail;
	}

	bool empty() const {
		return m_head == m_tail;
	}
};


struct CircularBuffer2 {
	const size_t m_reserved;
	const size_t m_capacity;

	size_t m_write;
	size_t m_read;

	CircularBuffer2(size_t N, size_t StartIndex = 0) : m_reserved(N+1), m_capacity(N) {
		m_write = StartIndex;
		m_read = StartIndex;
	}

	size_t push(size_t& out_offset) {
		printf("push: head %d, tail %d\n", m_write, m_read);
		out_offset = m_write;
		size_t next = m_write+1;
		if (next == m_reserved) next = 0;
		if (next == m_read) return 0;

		m_write = next;
		return 1;
	}

	size_t pop(size_t& out_offset) {
		printf("pop: head %d, tail %d\n", m_write, m_read);
		if (empty()) return 0;

		out_offset = m_read;
		size_t next = m_read + 1;
		if (next == m_reserved) next = 0;

		m_read = next;
		return 1;
	}

	size_t size() const {
		printf("size: head %d, tail %d\n", m_write, m_read);
		return m_write - m_read;
	}

	bool empty() const {
		return m_write == m_read;
	}
};

const int kVecSize = 128;

int main() {
	int N = 16*kVecSize;
	int xs[N];

#if 0
	CircularBuffer buf1(100, 0);

	for (int i=0; i<50; i++) {
		size_t off;
		size_t n = buf1.push(off);
		assert(n == 1);
	}

	assert(buf1.size() == 50);

	for (int i=0; i<51; i++) {
		size_t off;
		size_t n = buf1.push(off);

		if (i < 50)
			assert(n == 1);
		else
			assert(n == 0);
	}
	assert(buf1.size() == 100);

	size_t offset;
	assert(buf1.pop(offset));
	assert(offset == 0);
	assert(buf1.pop(offset));
	assert(offset == 1);

	assert(buf1.push(offset));
	assert(offset == 0);

	assert(buf1.push(offset));
	assert(offset == 1);

	assert(buf1.size() == 100);

	for (int i=0; i<50; i++) {
		CircularBuffer buf3(100, i);

		size_t offset;
		size_t num;

		num = buf3.push(offset, 50);
		assert(num == 50);
		assert(buf3.size() == 50);
		assert(!buf3.empty());

		num = buf3.push(offset, 51);
		assert(num == 50);
		assert(buf3.size() == 100);
		assert(!buf3.empty());

		num = buf3.push(offset, 1);
		assert(num == 0);
		assert(buf3.size() == 100);
		assert(!buf3.empty());

		assert(buf3.pop(offset));
		assert(offset == 0);
		assert(buf3.pop(offset));
		assert(offset == 1);

		num = buf3.push(offset, 2);
		assert(num == 2);
		assert(offset == 0);
	}
#endif
	for (int i=0; i<N; i++) {
		xs[i] = i;
	}

	for (int remove=0; remove < 16; remove++) {
		for (int b1 = 2; b1 <= 32; b1+=2) {
			for (int b2 = 2; b2 <= 32; b2+=2) {
				TestStack test1(b1, b2);
				test1(xs, N-remove);
			}
		}
	}
}