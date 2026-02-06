#ifndef Py_LIMITED_API
#error "Py_LIMITED_API must be defined! We rely on it to ensure we attempt to use stable 3.x ABI"
#endif
#include <Python.h>
#ifdef _LARGEFILE_SOURCE
#undef _LARGEFILE_SOURCE
#endif
#include <xdelta3/xdelta3.h>

#define BLOCK_SIZE 1 << 23
#define BLOCK_CACHE_SIZE 32

struct cache_nav {
  struct cache_nav *next;
  struct cache_nav *prev;
};

struct cache {
  xoff_t blkno;
  usize_t onblk;
  uint8_t *blk;
  struct cache_nav nav;
};

static inline struct cache* cache_entry(struct cache_nav *l) {
  return (struct cache*) ((char*) l - (ptrdiff_t) &((struct cache*) 0)->nav);
}

static inline void cache_init(struct cache_nav *l) {
  l->prev = l;
  l->next = l;
}
static inline void cache_add(struct cache_nav *prev, struct cache_nav *next,
                      struct cache_nav *ins) {
  next->prev = ins;
  prev->next = ins;
  ins->next = next;
  ins->prev = prev;
}

static inline struct cache* cache_pop_front(struct cache_nav *l) {
  struct cache_nav* i = l->next;
  i->next->prev = i->prev;
  i->prev->next = i->next;
  return cache_entry(i);
}

static inline struct cache* cache_remove(struct cache *f) {
  struct cache_nav *i = f->nav.next;
  f->nav.next->prev = f->nav.prev;
  f->nav.prev->next = f->nav.next;
  return cache_entry(i);
}


void put_progress(PyObject *queue, usize_t written, usize_t read) {
  PyObject *progress_tuple = NULL;
  PyObject *put_result = NULL;

  PyObject *written_obj = NULL;
  PyObject *read_obj = NULL;

  PyObject *put_method = PyObject_GetAttrString(queue, "put");
  if (!put_method || !PyCallable_Check(put_method)) {
    PyErr_SetString(PyExc_TypeError, "'put' is not callable");
    Py_XDECREF(put_method);
    return;
  }

  read_obj = PyLong_FromLong(read);
  written_obj = PyLong_FromLong(written);

  if (!written_obj || !read_obj) {
    Py_XDECREF(written_obj);
    Py_XDECREF(read_obj);
    Py_DECREF(put_method);
    return;
  }

  progress_tuple = PyTuple_New(2);
  if (!progress_tuple) {
    Py_DECREF(written_obj);
    Py_DECREF(read_obj);
    Py_DECREF(put_method);
    return;
  }

  PyTuple_SetItem(progress_tuple, 0, written_obj);
  PyTuple_SetItem(progress_tuple, 1, read_obj);

  put_result = PyObject_CallFunctionObjArgs(put_method, progress_tuple, NULL);

  Py_DECREF(put_method);
  Py_DECREF(progress_tuple);
  Py_DECREF(put_result);
}

static PyObject *patch(PyObject *self, PyObject *args) {
  const char *source;
  const char *patch;
  const char *target;
  PyObject *queue;

  xd3_stream stream;
  xd3_config config;
  xd3_source src;
  uint8_t *input_buffer = NULL;
  struct cache *block_cache = NULL;
  struct cache_nav block_cache_nav;

  FILE *fsource = NULL;
  FILE *fpatch = NULL;
  FILE *ftarget = NULL;

  usize_t input_read = 0;
  uint64_t offset = 0;
  usize_t cache_size = 0;

  usize_t written = 0;
  usize_t read = 0;


  if (!PyArg_ParseTuple(args, "sssO", &source, &patch, &target, &queue)) {
    return NULL;
  }
  if (!PyObject_HasAttrString(queue, "put")) {
    PyErr_SetString(PyExc_TypeError,
                    "Expected a queue-like object with a .put() method");
    return NULL;
  }
  cache_init(&block_cache_nav);
  input_buffer = malloc(BLOCK_SIZE);
  block_cache = malloc(BLOCK_CACHE_SIZE * sizeof(struct cache));
  if (!block_cache) {
    PyErr_SetFromErrno(PyExc_MemoryError);
    goto cleanup;
  }
  memset(block_cache, 0, sizeof(block_cache[0]) * BLOCK_CACHE_SIZE);
  block_cache[0].blk = malloc((BLOCK_SIZE) * BLOCK_CACHE_SIZE);
  if (!block_cache[0].blk) {
    PyErr_SetFromErrno(PyExc_MemoryError);
    goto cleanup;
  }
  cache_size = BLOCK_CACHE_SIZE;
  for (int i=0; i<cache_size;i++) {
    block_cache[i].blkno = -1;
    if (i>0) block_cache[i].blk = block_cache[0].blk + (i * BLOCK_SIZE);
    cache_add(block_cache_nav.prev, &block_cache_nav, &block_cache[i].nav);
  }

  Py_BEGIN_ALLOW_THREADS if (!(fsource = fopen(source, "rb"))) {
    Py_BLOCK_THREADS PyErr_SetFromErrno(PyExc_OSError);
    Py_UNBLOCK_THREADS goto cleanup;
  }

  if (!(fpatch = fopen(patch, "rb"))) {
    Py_BLOCK_THREADS PyErr_SetFromErrno(PyExc_OSError);
    Py_UNBLOCK_THREADS goto cleanup;
  }

  if (!(ftarget = fopen(target, "wb"))) {
    Py_BLOCK_THREADS PyErr_SetFromErrno(PyExc_OSError);
    Py_UNBLOCK_THREADS goto cleanup;
  }

  memset(&stream, 0, sizeof(stream));
  memset(&config, 0, sizeof(config));
  memset(&src, 0, sizeof(src));

  config.winsize = XD3_DEFAULT_WINSIZE;
  xd3_config_stream(&stream, &config);

  src.blksize = BLOCK_SIZE;
  src.curblk = block_cache[0].blk;
  src.curblkno = 0;
  block_cache[0].blkno = 0;
  src.onblk = fread(src.curblk, sizeof(uint8_t), BLOCK_SIZE, fsource);
  xd3_set_source(&stream, &src);
  block_cache[0].onblk = src.onblk;

  do {
    input_read = fread(input_buffer, sizeof(uint8_t), BLOCK_SIZE, fpatch);
    if (input_read < BLOCK_SIZE) {
      xd3_set_flags(&stream, XD3_FLUSH);
    }
    xd3_avail_input(&stream, input_buffer, input_read);
  process:
    switch (xd3_decode_input(&stream)) {
    case XD3_INPUT:
      continue;
    case XD3_OUTPUT:
      fwrite(stream.next_out, sizeof(uint8_t), stream.avail_out, ftarget);
      xd3_consume_output(&stream);
      goto process;
    case XD3_GETSRCBLK: {
      for (int i = 0; i < cache_size; i++) {
        if (block_cache[i].blkno == src.getblkno) {
          src.onblk = block_cache[i].onblk;
          src.curblk = block_cache[i].blk;
          src.curblkno = src.getblkno;
          cache_remove(&block_cache[i]);
          cache_add(block_cache_nav.prev, &block_cache_nav, &block_cache[i].nav);
          goto process;
        }
      }
      struct cache* cache_el = cache_pop_front(&block_cache_nav);
      cache_add(block_cache_nav.prev, &block_cache_nav, &cache_el->nav);

      offset = src.blksize * src.getblkno;
      fseek(fsource, offset, SEEK_SET);
      cache_el->onblk = fread(
          cache_el->blk, sizeof(uint8_t), src.blksize, fsource);
      cache_el->blkno = src.getblkno;

      src.curblkno = cache_el->blkno;
      src.onblk = cache_el->onblk;
      src.curblk = cache_el->blk;

      goto process;
    }
    case XD3_GOTHEADER:
    case XD3_WINSTART:
    case XD3_WINFINISH:
      /* no action necessary */
      Py_BLOCK_THREADS
      put_progress(queue, stream.total_out - written, stream.total_in - read);
      written = stream.total_out;
      read = stream.total_in;
      Py_UNBLOCK_THREADS
      goto process;
    default:
      Py_BLOCK_THREADS if (stream.msg) {
        printf("%s\n", stream.msg);
        fflush(stdout);
      }
      PyErr_SetFromErrno(PyExc_MemoryError);
      Py_UNBLOCK_THREADS goto cleanup;
    }

  } while (input_read == BLOCK_SIZE);
  if (xd3_close_stream(&stream)) {
    Py_BLOCK_THREADS PyErr_SetFromErrno(PyExc_AssertionError);
    Py_UNBLOCK_THREADS
  }

cleanup:
  Py_END_ALLOW_THREADS xd3_free_stream(&stream);
  if (block_cache) {
    if (block_cache[0].blk) free(block_cache[0].blk);
    free(block_cache);
  }

  if (input_buffer)
    free(input_buffer);
  if (fsource)
    fclose(fsource);
  if (fpatch)
    fclose(fpatch);
  if (ftarget) {
    fflush(ftarget);
    fclose(ftarget);
  }

  Py_RETURN_NONE;
}

static PyMethodDef methods[] = {
    {"patch", patch, METH_VARARGS, "Runs a patch on provided files"},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef xdelta_def = {PyModuleDef_HEAD_INIT, "xdelta3", NULL,
                                        -1, methods};

PyMODINIT_FUNC PyInit_xdelta3(void) { return PyModule_Create(&xdelta_def); }