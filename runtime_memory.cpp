#include "runtime_memory.hpp"
#include "runtime.hpp"
#include "build.hpp"

#define HAVE_LINUX_MREMAP

#ifdef HAVE_POSIX_SYSCONF
#include <unistd.h>
#endif

static size_t
get_page_size()
{
	size_t pagesize;
#ifdef HAVE_POSIX_SYSCONF
	pagesize = sysconf(_SC_PAGESIZE);
#else
	// some huge number
	pagesize = 2 * 1024 * 1024;
#endif

	ASSERT(pagesize > 0);
	return pagesize;
}


static bool
malloc_allocex(size_t alloc_size, size_t page_size, size_t *out_size,
	void **out)
{
	(void)page_size;
	*out_size = alloc_size;
	*out = malloc(alloc_size);
	return true;
}

static bool
malloc_reallocex(size_t page_size, void *old_ptr, size_t old_size,
	size_t new_size, size_t *out_size, void **out_ptr)
{
	(void)page_size;
	(void)old_size;
	*out_size = new_size;
	*out_ptr = realloc(old_ptr, new_size);
	return true;
}

static void
malloc_dealloc(void *p, size_t alloc_size)
{
	(void)alloc_size;
	::free(p);
}

#ifdef HAVE_POSIX_MMAP
#include <sys/mman.h>
#include <fcntl.h>

static bool
mmap_allocex(size_t alloc_size, size_t page_size, size_t *out_size,
	void **out)
{
	void *p;
	int fd;
	int flags = MAP_PRIVATE;
	size_t fixed_size;

	fixed_size = (alloc_size + page_size - 1) / page_size * page_size;

#ifdef HAVE_LINUX_MAP_POPULATE
	flags |= MAP_POPULATE;
#endif

	fd = open("/dev/zero", O_RDWR);
	p = mmap(0, fixed_size, PROT_READ|PROT_WRITE, flags, fd, 0);
	close(fd);

	if (p == MAP_FAILED) {
		return false;
	}

	*out_size = fixed_size;
	*out = p;

	return true;
}

static void
mmap_dealloc(void *p, size_t alloc_size)
{
	if (p) {
		munmap(p, alloc_size);
	}
}
static bool
mmap_reallocex(size_t page_size, void *old_ptr, size_t old_size,
	size_t new_size, size_t *out_size, void **out_ptr)
{
#ifdef HAVE_LINUX_MREMAP
	ASSERT(new_size > 0);
	ASSERT(page_size > 0);
	ASSERT(old_ptr);
	ASSERT((size_t)old_ptr % page_size == 0);
	ASSERT(old_size % page_size == 0);
	const size_t fixed_size = ((new_size + page_size - 1) / page_size) * page_size;
	void* p = mremap(old_ptr, old_size, fixed_size, MREMAP_MAYMOVE);
	if (p == MAP_FAILED) {
		printf("mremap(%p, %lld, %lld, MREMAP_MAYMOVE | MREMAP_FIXED, %p) with newsize=%lld failed %s\n",
			old_ptr, old_size, fixed_size, old_ptr, new_size, strerror(errno));
		ASSERT(false);
		return false;
	}
	*out_size = fixed_size;
	*out_ptr = p;
	return true;
#else
	bool r = mmap_allocex(new_size, page_size, out_size, out_ptr);
	if (!r) {
		return false;
	}

	memcpy(*out_ptr, old_ptr, old_size);
	mmap_dealloc(old_ptr, old_size);
	return true;
#endif
}

#else
static bool
mmap_allocex(size_t alloc_size, size_t page_size, size_t *out_size,
	void **out)
{
	return malloc_allocex(alloc_size, page_size, out_size, out);
}

static bool
mmap_reallocex(size_t page_size, void *old_ptr, size_t old_size,
	size_t new_size, size_t *out_size, void **out_ptr)
{
	return malloc_reallocex(page_size, old_ptr, old_size,
		new_size, out_size, out_ptr);
}

static void
mmap_dealloc(void *p, size_t alloc_size)
{
	malloc_dealloc(p, alloc_size);
}
#endif


LargeBuffer::LargeBuffer()
{
	_size = 0;
	_data = nullptr;
	_page_size = get_page_size();
	reset();
}

void
LargeBuffer::alloc(size_t size)
{
	ASSERT(!_size && !_data);
	_size = size;

	bool r = mmap_allocex(_size, _page_size, &_size, &_data);
	ASSERT(r);
	reset();
}

void
LargeBuffer::free()
{
	if (_data) {
		mmap_dealloc(_data, _size);
	}
	_data = nullptr;
	_size = 0;
}

LargeBuffer::~LargeBuffer()
{
	this->free();
}

void
LargeBuffer::resize(size_t newsz)
{
	bool r = mmap_reallocex(_page_size, _data, _size, newsz,
		&_size, &_data);
	ASSERT(r);
	_size = newsz;
}

void
LargeBuffer::reset()
{
	if (_data) {
		memset(_data, 0, _size);
	}
}