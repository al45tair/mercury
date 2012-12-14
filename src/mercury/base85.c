#define PY_SSIZE_T_CLEAN
#include <Python.h>

static const char base85[] = 
  "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  "abcdefghijklmnopqrstuvwxyz!#$%&()*+-"
  ";<=>?@^_`{|}~";
static signed char base85_dec[256];

static PyObject *
base85_encode(PyObject *self, PyObject *args)
{
  const unsigned char *data;
  int should_pad = 0;
  PyObject *result;
  const unsigned char *dptr, *dend;
  char *rptr, *rend;
  Py_ssize_t dlen, rlen;

  if (!PyArg_ParseTuple (args, "s#|i", &data, &dlen, &should_pad))
    return NULL;

  if (should_pad)
    rlen = (dlen + 3) / 4 * 5;
  else
    rlen = (dlen * 5 + 3) / 4;
  
  result = PyBytes_FromStringAndSize (NULL, rlen);

  if (!result)
    return NULL;

  rptr = PyBytes_AsString (result);
  rend = rptr + rlen;

  dptr = data;
  dend = dptr + dlen;

  while (dptr < dend) {
    unsigned word = 0;
    char chars[5];

    word |= *dptr++ << 24;
    if (dptr < dend)
      word |= *dptr++ << 16;
    if (dptr < dend)
      word |= *dptr++ << 8;
    if (dptr < dend)
      word |= *dptr++;

    chars[4] = base85[word % 85]; word /= 85;
    chars[3] = base85[word % 85]; word /= 85;
    chars[2] = base85[word % 85]; word /= 85;
    chars[1] = base85[word % 85]; word /= 85;
    chars[0] = base85[word];

    *rptr++ = chars[0];
    if (rptr < rend)
      *rptr++ = chars[1];
    if (rptr < rend)
      *rptr++ = chars[2];
    if (rptr < rend)
      *rptr++ = chars[3];
    if (rptr < rend)
      *rptr++ = chars[4];
  }

  return result;
}

static PyObject *
base85_decode(PyObject *self, PyObject *args)
{
  const unsigned char *text;
  PyObject *result;
  Py_ssize_t tlen, rlen;
  unsigned char *rptr, *rend;
  const unsigned char *tptr, *tend;

  if (!PyArg_ParseTuple (args, "s#", &text, &tlen))
    return NULL;

  rlen = (tlen * 4) / 5;

  result = PyBytes_FromStringAndSize (NULL, rlen);

  if (!result)
    return NULL;

  rptr = (unsigned char *)PyBytes_AsString (result);
  rend = rptr + rlen;

  tptr = text;
  tend = tptr + tlen;

  while (tptr < tend) {
    unsigned word = 0;
    int val;
    
    val = base85_dec[*tptr++];

    if (val < 0)
      goto bad;

    word = val;

    word *= 85;
    if (tptr < tend) {
      val = base85_dec[*tptr++];

      if (val < 0)
        goto bad;

      word += val;
    }

    word *= 85;
    if (tptr < tend) {
      val = base85_dec[*tptr++];

      if (val < 0)
        goto bad;

      word += val;
    }

    word *= 85;
    if (tptr < tend) {
      val = base85_dec[*tptr++];

      if (val < 0)
        goto bad;

      word += val;
    }

    if (word > 0x03030303)
      goto bad;

    word *= 85;
    if (tptr < tend) {
      val = base85_dec[*tptr++];

      if (val < 0)
        goto bad;

      if (word > 0xffffffff - val)
        goto bad;

      word += val;
    }

    // We want to round UP
    if (rend - rptr < 4)
      word += 0xffffff >> (rend - rptr - 1) * 8;

    *rptr++ = (unsigned char)(word >> 24);
    if (rptr < rend)
      *rptr++ = (unsigned char)(word >> 16);
    if (rptr < rend)
      *rptr++ = (unsigned char)(word >> 8);
    if (rptr < rend)
      *rptr++ = (unsigned char)(word);
  }

  return result;

bad:
  PyErr_SetString(PyExc_ValueError, "bad base85");
  Py_DECREF (result);
  return NULL;
}

static char base85_doc[] = "Base85 data encoding";

static PyMethodDef methods[] = {
  { "base85_encode", base85_encode, METH_VARARGS,
    "base85_encode(data[, should_pad]) -> string\n"
    "\n"
    "Encode data in base85, returning a string result.  If should_pad is True,\n"
    "the result will be padded to a multiple of five characters.\n" },
  { "base85_decode", base85_decode, METH_VARARGS,
    "base85_decode(string) -> data\n"
    "\n"
    "Decode data in base85, returning a byte string result.\n" },
  { NULL }
};

static void
base85_init(void)
{
  unsigned n;
  memset (base85_dec, -1, sizeof (base85_dec));
  for (n = 0; n < sizeof (base85); ++n)
    base85_dec[(unsigned)base85[n]] = n;
}

#ifdef IS_PY3K
static struct PyModuleDef base85_module = {
  PyModuleDef_HEAD_INIT,
  "base85",
  base85_doc,
  -1,
  methods
};

PyMODINIT_FUNC
PyInit_base85(void)
{
  base85_init();

  return PyModule_Create(&base85_module);
}
#else
PyMODINIT_FUNC
initbase85(void)
{
  base85_init();

  Py_InitModule3("base85", methods, base85_doc);
}
#endif
