#define TEST_BUFFERING

#if 0
#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "catch.hpp"
#endif

#include "test_buffering_helper.hpp"
const char* g_pipeline_name = "test_buffering";

void reset_watermark(size_t x) {
	g_print_debug = false;

	printf("%s: to %d\n", __func__, (int)x);
}

void set_watermark(size_t x) {
	g_print_debug = true;
	printf("%s: to %d\n", __func__, (int)x);
}

template<size_t READ, size_t WRITE>
struct Test1 {
	void test1() {
		static TestSimpleBuf<READ, WRITE> _bufA;
		static TestSimpleBuf<WRITE, READ> _bufB;

	    ITestSimpleBuf& bufA = _bufA;
	    ITestSimpleBuf& bufB = _bufB;

	    int state = 0;

	    for (int iter=0; iter<1000000; iter++) {
	    	switch (state) {
	    	case 0:
	    		bufA.write();
	    		if (bufA.shall_flush()) {
	    			state = 1;
	    		}
	    		break;

	    	case 1:
	    		{
					if (bufA.shall_refill()) {
						state = 0;
						break;
					}
					auto r = bufA.read();
					bufB.fill(r.len);

					if (bufB.shall_flush()) {
						state = 2;
						break;
					}
					break;
			    }

	    	case 2:
	    		{
					if (bufB.shall_refill()) {
						state = 1;
						break;
					}
					auto r = bufB.read();
					bufA.fill(r.len);

					if (bufA.shall_flush()) {
						state = 1;
						break;
					}
					break;
	    		}
	    	}
	    }
	}

	void operator()() {
		g_print_debug = false;
		printf("TEST1<%d, %d>\n", (int)READ, (int)WRITE);
		test1();
	}
};

int main() {
#define EXPAND(A, ARGS) A(1, 25, ARGS); A(1, 1023, ARGS); A(4, 25, ARGS); \
	A(4, 1033, ARGS); \
	/* A(1021, 1, ARGS); A(33, 3335, ARGS); */

#define A(WRITE, READ, _) {Test1<WRITE, READ> t; t(); }
	EXPAND(A, 0);

	return 0;
}