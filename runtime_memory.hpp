#ifndef H_RUNTIME_MEMORY
#define H_RUNTIME_MEMORY

#include <string>

struct LargeBuffer {
	LargeBuffer();

	void alloc(size_t size);
	void free();

	~LargeBuffer();
	void resize(size_t newsz);

	template<typename T>
	T* get() {
		return (T*)_data;
	}

	void reset();

	size_t size() const {
		return _size;
	}
private:
	void* _data;
	size_t _size;
	size_t _page_size;
};

#endif