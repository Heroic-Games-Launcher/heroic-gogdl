#include <xdelta3/xdelta3.h>
#ifdef _LARGEFILE_SOURCE
#undef _LARGEFILE_SOURCE
#endif
#include <Python.h>

#define WINDOW_SIZE 1 << 20

void put_progress(PyObject *queue, usize_t written, usize_t read) {
  PyObject *progress_tuple;
  PyObject *put_result;

  PyObject *put_method = PyObject_GetAttrString(queue, "put");
  if (!put_method || !PyCallable_Check(put_method)) {
    PyErr_SetString(PyExc_TypeError, "'put' is not callable");
    Py_XDECREF(put_method);
    return;
  }

  progress_tuple = PyTuple_New(2);
  if (!progress_tuple)
    return;

  PyTuple_SetItem(progress_tuple, 0, PyLong_FromLong(written));
  PyTuple_SetItem(progress_tuple, 1, PyLong_FromLong(read));
  put_result = PyObject_CallFunctionObjArgs(put_method, progress_tuple, NULL);

  Py_DECREF(put_method);
  Py_DECREF(progress_tuple);
  Py_XDECREF(put_result);
}

static PyObject *patch(PyObject *self, PyObject *args) {
  const char *source;
  const char *patch;
  const char *target;
  PyObject *queue;

  xd3_stream stream;
  xd3_config config;
  xd3_source src;
  uint8_t source_block[WINDOW_SIZE];
  uint8_t input_buffer[WINDOW_SIZE];

  FILE *fsource = NULL;
  FILE *fpatch = NULL;
  FILE *ftarget = NULL;

  usize_t input_read = 0;

  if (!PyArg_ParseTuple(args, "sssO", &source, &patch, &target, &queue)) {
    return NULL;
  }
  if (!PyObject_HasAttrString(queue, "put")) {
    PyErr_SetString(PyExc_TypeError,
                    "Expected a queue-like object with a .put() method");
    return NULL;
  }

  if (!(fsource = fopen(source, "r"))) {
    PyErr_SetFromErrno(PyExc_OSError);
    goto cleanup;
  }

  if (!(fpatch = fopen(patch, "r"))) {
    PyErr_SetFromErrno(PyExc_OSError);
    goto cleanup;
  }

  if (!(ftarget = fopen(target, "w"))) {
    PyErr_SetFromErrno(PyExc_OSError);
    goto cleanup;
  }

  memset(&stream, 0, sizeof(stream));
  memset(&config, 0, sizeof(config));
  memset(&src, 0, sizeof(src));

  config.winsize = WINDOW_SIZE;

  xd3_config_stream(&stream, &config);

  src.blksize = WINDOW_SIZE;
  src.curblk = source_block;
  src.curblkno = 0;
  src.onblk = fread(source_block, sizeof(uint8_t), WINDOW_SIZE, fsource);
  xd3_set_source(&stream, &src);

  do {
    input_read = fread(input_buffer, sizeof(uint8_t), WINDOW_SIZE, fpatch);
    if (input_read < WINDOW_SIZE) {
      xd3_set_flags(&stream, XD3_FLUSH);
    }
    xd3_avail_input(&stream, input_buffer, input_read);
  process:

    switch (xd3_decode_input(&stream)) {
    case XD3_INPUT:
      continue;
    case XD3_OUTPUT:
      fwrite(stream.next_out, sizeof(uint8_t), stream.avail_out, ftarget);
      put_progress(queue, stream.avail_out, 0);
      xd3_consume_output(&stream);
      goto process;
    case XD3_GETSRCBLK:
      usize_t offset = src.blksize * src.getblkno;
      fseek(fsource, offset, SEEK_SET);
      src.onblk = fread(source_block, sizeof(uint8_t), WINDOW_SIZE, fsource);
      src.curblkno = src.getblkno;
      put_progress(queue, 0, src.onblk);
      goto process;
    case XD3_GOTHEADER:
    case XD3_WINSTART:
    case XD3_WINFINISH:
      /* no action necessary */
      goto process;
    default:
      PyErr_SetFromErrno(PyExc_MemoryError);
      goto cleanup;
    }

  } while (input_read == WINDOW_SIZE);
  if (xd3_close_stream(&stream)) {
    PyErr_SetFromErrno(PyExc_AssertionError);
  }
cleanup:
  xd3_free_stream(&stream);
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