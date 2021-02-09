#include "ring_buffer.hpp"

template<typename T>
size_t write1(T& b, size_t& offset) {
	size_t r = b.write_begin(offset, 1);
	b.write_end(r);
	return r;
}

template<typename T>
size_t read1(T& b, size_t& offset) {
	size_t r = b.read_begin(offset, 1);
	b.read_end(r);
	return r;
}

template<bool CONC_READ, bool CONC_WRITE>
static void
__test1(size_t capacity)
{
	assert(capacity % 2 == 0);

	GenericBuffer<CONC_READ, CONC_WRITE> buf1(capacity, "buf1");
	GenericBuffer<CONC_READ, CONC_WRITE> buf2(capacity, "buf2");

	size_t off1, off2, num1, num2;


	auto fill_buffer = [&] (size_t cap, size_t start_size) {
		for (size_t i=0; i<cap; i++) {
			assert(buf1.size() == start_size+i); 

			size_t n = write1(buf1, off1);
			assert(n == 1);	
			assert(!buf1.empty());

			if (i % 2 == 0) {
				assert(buf2.size() == start_size+i); 

				size_t n = buf2.write_begin(off2, 2);
				buf2.write_end(n);
				assert(n == 2);	
				assert(!buf2.empty());			

				assert(off2 == off1);
			}
		}

	};



	{
		assert(buf1.empty());
		size_t n = read1(buf1, off1);
		assert(n == 0);
	}

	{
		size_t n = buf2.read_begin(off2, 2);
		buf2.read_end(n);
		assert(n == 0);	
	}

	assert(off2 == off1);

	// fill buffer completely
	fill_buffer(capacity, 0);

	// full 
	num1 = write1(buf1, off1);
	assert(num1 == 0);

	num2 = buf2.write_begin(off2, 2);
	buf2.write_end(num2);
	assert(num2 == 0);

	for (int k=0; k<3; k++) {
		// read only 50%
		for (size_t i=0; i<capacity/2; i++) {
			size_t n = read1(buf1, off1);
			assert(n == 1);
			assert(!buf1.empty());

			if (i % 2 == 0) {
				size_t n = buf2.read_begin(off2, 2);
				buf2.read_end(n);
				assert(n == 2);	
				assert(!buf2.empty());

				assert(off2 == off1);
			}
		}

		// fill the 50%
		fill_buffer(capacity/2, capacity-capacity/2);
	}
}

void
test1(size_t capacity)
{
	__test1<true, true>(capacity);
	__test1<true, false>(capacity);
	__test1<false, true>(capacity);
	__test1<false, false>(capacity);
}

void
testN()
{
	GenericBuffer<true, true> buf(5, "buf");

	size_t offA, numA, offB, numB, offC, numC;

	// <A>
	numA = buf.write_begin(offA, 1);
	assert(numA == 1);
	assert(buf.size() == 0);
	assert(buf.empty());

	// <B>
	numB = buf.write_begin(offB, 2);
	assert(numB == 2);
	assert(buf.size() == 0);
	assert(buf.empty());

	// <A>
	buf.write_end(numA);
	assert(buf.size() == 1);
	assert(!buf.empty());

	// <B>
	buf.write_end(numB);
	assert(buf.size() == 1+2);
	assert(!buf.empty());

	// ====
	assert(offA == 0);
	assert(offB == 1);



	// A writes, B reads
	numA = buf.write_begin(offA, 2);
	assert(numA == 2);

	numB = buf.read_begin(offB, 3);
	assert(numB == 3);

	buf.write_end(numA);
	buf.read_end(numB);

	assert(buf.size() == 2);
	assert(!buf.empty());


	// A reads 2, B writes 1
	numA = buf.read_begin(offA, 2);
	assert(numA == 2);	

	numB = buf.write_begin(offB, 1);
	assert(numB == 1);

	buf.read_end(numA);
	buf.write_end(numB);

	assert(buf.size() == 1);



	numA = buf.read_begin(offA, 2);
	assert(numA == 1);

	numB = buf.read_begin(offB, 1);
	assert(numB == 0);

	buf.read_end(numB);
	buf.read_end(numA);	
}


int main() {
	testN();
	
	test1(8);
	test1(16);

	return 0;
}