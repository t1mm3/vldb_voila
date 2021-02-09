#ifndef H_TEST_BUFFERING_HELPER
#define H_TEST_BUFFERING_HELPER

#include <memory>
#include <cassert>

extern bool g_print_debug;

#define ASSERT(x) assert(x)
#define LOG_DEBUG(...) if (g_print_debug) { printf(__VA_ARGS__); }
#define LOG_TRACE(...)

#define LOG_ERROR(...) printf(__VA_ARGS__);

#include "runtime_buffering.hpp"

template<size_t READ_GRAN, size_t WRITE_GRAN>
struct WrapSimpleBuf : SimpleBuf<WRITE_GRAN*2, 0, READ_GRAN, WRITE_GRAN, std::max(WRITE_GRAN*4, std::min(READ_GRAN*4, (size_t)128))>
{

};

struct ITestSimpleBuf {
	struct Pos {
		size_t off;
		size_t len;
	};

	const size_t kWriteGran;
	const size_t kReadGran;
	ITestSimpleBuf(size_t wgran, size_t rgran) : kWriteGran(wgran), kReadGran(rgran) {

	}

	~ITestSimpleBuf() {

	}

	virtual Pos read(size_t num) = 0;

	virtual Pos read() {
		return read(kReadGran);
	}

	virtual Pos write(size_t num) = 0;
	
	virtual Pos write() {
		return write(kWriteGran);
	}

	virtual bool empty() = 0;

	virtual void fill(size_t num) {
		for (size_t i=0; i<num; i+= kWriteGran) {
			size_t n = std::min(num - i, kWriteGran);

			write(n);
		}
	}

	virtual bool shall_flush() = 0;
	virtual bool shall_refill() = 0;

	void consume(size_t num) {
		while (!empty()) {
			read(num);
		}
	}

	// static std::unique_ptr<ITestSimpleBuf> create(size_t r, size_t w);

};

template<size_t READ_GRAN, size_t WRITE_GRAN>
struct TestSimpleBuf : ITestSimpleBuf
{
	typedef WrapSimpleBuf<READ_GRAN, WRITE_GRAN> Base;

	TestSimpleBuf() : ITestSimpleBuf(WRITE_GRAN, READ_GRAN) {
	}

	Base base;
	
	Pos read(size_t num) override {
		typename Base::ReadContext ctx;

		base.read_begin(ctx, num);
		base.read_commit(ctx);

		return Pos {ctx.offset, ctx.length};
	}

	Pos write(size_t num) override {
		typename Base::WriteContext ctx;

		base.write_begin(ctx, num);
		base.write_commit(ctx);

		return Pos {ctx.offset, ctx.length};
	}

	bool empty() override {
		return base.is_empty();
	}

	virtual bool shall_flush() override {
		return base.control_shall_flush();
	}

	virtual bool shall_refill() override {
		return base.control_shall_refill();
	}
};

#if 0
int test1() {
    TestSimpleBuf<25,1> buf;

    for (int iter=0; iter<1000; iter++) {
    	buf.write();

    	while (buf.shall_flush()) {
    		buf.read();
    	}
    }

    while (buf.control_shall_flush()) {
		buf.read();
	}

    ASSERT(buf.is_empty());

    return 0;
}
#endif

#endif