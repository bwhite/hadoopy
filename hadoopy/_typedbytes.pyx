#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import types
import cPickle as pickle
from libc.stdint cimport int32_t, int64_t

cdef extern from "stdlib.h":
    void *malloc(size_t size)
    void free(void *ptr)            

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, void *stream)
    void *stdin
    void *stdout
    void *stderr
    int getc(void *stream)
    size_t fread(void *ptr, size_t size, size_t nmemb, void *stream)
    size_t fwrite(void *ptr, size_t size, size_t nmemb, void *stream)
    void *fdopen(int fd, char *mode)
    int fclose(void *fp)
    void *fopen(char *path, char *mode)
    int fflush(void *stream)

cdef extern from "byteconversion.h":
    int32_t _be32toh(int32_t val)
    int64_t _be64toh(int64_t val)
    int32_t _htobe32(int32_t val)
    int64_t _htobe64(int64_t val)

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)
    int PyString_AsStringAndSize(object obj, char **buffer, Py_ssize_t *length)
    char* PyString_AsString(object string)
    

cdef inline int32_t _read_int(void *fp):
    """Read integer

    Code: 3
    Format: <32-bit signed integer>

    Returns:
        Python int
    """
    cdef int32_t val
    fread(&val, 4, 1, fp)  # = 1
    return _be32toh(val)


cdef inline _raw_write_int(void *fp, val):
    """Write integer (used for sizes)

    Args:
        val: Python int

    Raises:
        OverflowError: If val overflows an int
    """
    cdef int32_t cval = val
    cval = _htobe32(cval)
    fwrite(&cval, 4, 1, fp)  # = 1


cdef inline _write_int(void *fp, val):
    """Write integer

    Code: 3
    Format: <32-bit signed integer>

    Args:
        val: Python int
    """
    cdef int32_t cval
    try:
        cval = val
    except OverflowError:
        return _write_long(fp, val)
    cval = _htobe32(cval)
    fwrite(&cval, 4, 1, fp)  # = 1


cdef inline int64_t _read_long(void *fp):
    """Read integer

    Code: 4
    Format: <64-bit signed integer>

    Returns:
        Python int
    """
    cdef int64_t val
    fread(&val, 8, 1, fp)  # = 1
    return _be64toh(val)


cdef inline _write_long(void *fp, val):
    """Write long

    Code: 4
    Format: <64-bit signed integer>

    Args:
        val: Python int
    """
    cdef int64_t cval = val
    cval = _htobe64(cval)
    fwrite(&cval, 8, 1, fp)  # = 1


cdef inline float _read_float(void *fp):
    """Read float

    Code: 5
    Format: <32-bit IEEE floating point number>

    Returns:
        Python float
    """
    cdef int32_t val
    fread(&val, 4, 1, fp)  # = 1
    val = _be32toh(val)
    return (<float*>&val)[0]


cdef inline _write_float(void *fp, val):
    """Write float

    Code: 5
    Format: <32-bit IEEE floating point number>

    Args:
        val: Python float
    """
    cdef float cval = val
    cdef int32_t cvalo = _htobe32((<int32_t*>&cval)[0])
    fwrite(&cvalo, 4, 1, fp)  # = 1


cdef inline double _read_double(void *fp):
    """Read double

    Code: 6
    Format: <64-bit IEEE floating point number>

    Returns:
        Python float
    """
    cdef int64_t val
    fread(&val, 8, 1, fp)  # = 1
    val = _be64toh(val)
    return (<double*>&val)[0]


cdef inline _write_double(void *fp, val):
    """Write double

    Code: 6
    Format: <64-bit IEEE floating point number>

    Args:
        val: Python float
    """
    cdef double cval = val
    cdef int64_t cvalo = _htobe64((<int64_t*>&cval)[0])
    fwrite(&cvalo, 8, 1, fp)  # = 1


cdef inline _read_byte(void *fp):
    """Read byte

    Code: 1
    Format: <signed byte>

    Returns:
        Python int
    """
    cdef signed char val
    fread(&val, 1, 1, fp)  # = 1
    return int(val)


cdef inline _write_byte(void *fp, val):
    """Write byte

    Code: 1
    Format: <signed byte>

    Args:
        val: Python int
    """
    cdef signed char cval = val
    fwrite(&cval, 1, 1, fp)  # = 1
    

cdef inline _read_bool(void *fp):
    """Read integer

    Code: 2
    Format: <signed byte (0 = false and 1 = true)>

    Returns:
        Python Bool
    """
    return bool(_read_byte(fp))


cdef inline _write_bool(void *fp, val):
    """Write bool

    Code: 2
    Format: <signed byte (0 = false and 1 = true)>

    Args:
        val: Python bool
    """
    cdef signed char cval = val
    fwrite(&cval, 1, 1, fp)  # = 1


cdef inline _read_bytes(void *fp):
    """Read bytes

    Code: 0
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Returns:
        Python string of bytes
    """
    sz = _read_int(fp)
    cdef char *bytes = <char*>malloc(sz)
    fread(bytes, sz, 1, fp)  # = 1
    out = PyString_FromStringAndSize(bytes, sz)
    free(bytes)
    return out


cdef inline _write_bytes(void *fp, val):
    """Write bytes

    Code: 0
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Args:
        val: Python string (str or unicode)
    """
    cdef char *bytes
    cdef Py_ssize_t sz
    PyString_AsStringAndSize(val, &bytes, &sz)  # != -1
    _raw_write_int(fp, sz)
    fwrite(bytes, sz, 1, fp)  # = 1


cdef inline _read_string(void *fp):
    """Read string

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Returns:
        Python string
    """
    return _read_bytes(fp)


cdef inline _write_string(void *fp, val):
    """Write string

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Args:
        val: Python string
    """
    _write_bytes(fp, val)


cdef inline _read_vector(void *fp):
    """Read fixed length vector of typedbytes

    Code: 8
    Format: <32-bit signed integer> <as many typed bytes sequences as indicated by the integer>

    Returns:
        Python tuple with nested values
    """
    sz = _read_int(fp)
    out = []
    for x in range(sz):
        out.append(_read_tb_code(fp))
    return tuple(out)


cdef inline _write_vector(void *fp, val):
    """Write fixed length vector of typedbytes

    Code: 8
    Format: <32-bit signed integer> <as many typed bytes sequences as indicated by the integer>

    Args:
        val: Python tuple with nested values
    """
    cdef int sz = len(val)
    _raw_write_int(fp, sz)
    for x in val:
        _write_tb_code(fp, x)


cdef inline _read_list(void *fp):
    """Read variable length list of typedbytes

    Code: 9
    Format: <variable number of typed bytes sequences> <255 written as an unsigned byte>

    Returns:
        Python list of nested values
    """
    out = []
    while True:
        try:
            out.append(_read_tb_code(fp))
        except StopIteration:
            break
    return out


cdef inline _write_list(void *fp, val):
    """Write variable length list of typedbytes

    Code: 9
    Format: <variable number of typed bytes sequences> <255 written as an unsigned byte>

    Args:
        val: Python list of nested values
    """
    for x in val:
        _write_tb_code(fp, x)
    cdef unsigned char code = 255
    fwrite(&code, 1, 1, fp)  # = 1


cdef inline _read_map(void *fp):
    """Read fixed length pairs of typedbytes (interpreted as a dict/map)

    Code: 10
    Format: <32-bit signed integer> <as many (key-value) pairs of typed bytes sequences as indicated by the integer>

    Returns:
        Python dict with nested values
    """
    sz = _read_int(fp)
    out = {}
    for x in range(sz):
        k, v = _read_tb_code(fp), _read_tb_code(fp)
        out[k] = v
    return out


cdef inline _write_map(void *fp, val):
    """Write fixed length pairs of typedbytes (interpreted as a dict/map)

    Code: 10
    Format: <32-bit signed integer> <as many (key-value) pairs of typed bytes sequences as indicated by the integer>

    Args:
        val: Python dict with nested values
    """
    _raw_write_int(fp, len(val))
    for x, y in val.iteritems():
        _write_tb_code(fp, x)
        _write_tb_code(fp, y)


cdef inline _read_pickle(void *fp):
    """Read a python pickle

    Code: 100 (custom)
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Returns:
        Python object
    """
    return pickle.loads(_read_bytes(fp))


cdef inline _write_pickle(void *fp, val):
    """Write a python pickle

    Code: 100 (custom)
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Args:
        val: Python object
    """
    _write_bytes(fp, pickle.dumps(val, -1))


# 0: _write_bytes unused
# 1: _write_byte unused
# 5: _write_float unused
_out_types = {types.BooleanType: 2,
              types.IntType: 3,
              types.LongType: 4,
              types.FloatType:  6,
              types.StringType: 7,
              types.UnicodeType: 7,
              types.TupleType: 8,
              types.ListType: 9,
              types.DictType: 10}


cdef _write_tb_code(void *fp, val):
    cdef int type_code
    try:
        type_code = _out_types[type(val)]
    except KeyError:
        type_code = 100
    if type_code == 3 and (val < -2147483648 or 2147483647 < val):
        type_code = 4
    if type_code == 4 and (val < -9223372036854775808L or 9223372036854775807L < val):
        type_code = 100
    fwrite(&type_code, 1, 1, fp)  # = 1
    # TODO Use a func pointer array
    if type_code == 2:
        _write_bool(fp, val)
    elif type_code == 3:
        _write_int(fp, val)
    elif type_code == 4:
        _write_long(fp, val)
    elif type_code == 6:
        _write_double(fp, val)
    elif type_code == 7:
        _write_string(fp, val)
    elif type_code == 8:
        _write_vector(fp, val)
    elif type_code == 9:
        _write_list(fp, val)
    elif type_code == 10:
        _write_map(fp, val)
    elif type_code == 100:
        _write_pickle(fp, val)
    else:
        raise IndexError('Bad index %d ' % type_code)


cdef _read_tb_code(void *fp):
    cdef int type_code = getc(fp)
    # TODO Use a func pointer array
    if type_code == 0:
        return _read_bytes(fp)
    elif type_code == 1:
        return _read_byte(fp)
    elif type_code == 2:
        return _read_bool(fp)
    elif type_code == 3:
        return _read_int(fp)
    elif type_code == 4:
        return _read_long(fp)
    elif type_code == 5:
        return _read_float(fp)
    elif type_code == 6:
        return _read_double(fp)
    elif type_code == 7:
        return _read_bytes(fp)
    elif type_code == 8:
        return _read_vector(fp)
    elif type_code == 9:
        return _read_list(fp)
    elif type_code == 10:
        return _read_map(fp)
    elif type_code == 100:
        return _read_pickle(fp)
    elif type_code == 255:
        raise StopIteration
    elif type_code < 0:
        raise StopIteration        
    else:
        raise IndexError('Bad index %d ' % type_code)


cdef __read_key_value_tb(void *fp):
    k = _read_tb_code(fp)
    v = _read_tb_code(fp)
    return k, v


cdef __write_key_value_tb(void *fp, kv):
    k, v = kv
    _write_tb_code(fp, k)
    _write_tb_code(fp, v)
    

def read_tb():
    return __read_key_value_tb(stdin)


def write_tb(kv):
    __write_key_value_tb(stdout, kv)


cdef class TypedBytesFile(object):
    """TypedBytes interface

    :param fn: File path (default None)
    :param mode: Mode to open the file with (default None)
    :param read_fd: Read file descriptor (int) (default None)
    :param write_fd: Write file descriptor (int) (default None)
    :param flush_writes: If True then flush the buffer for every write (default False)
    """
    cdef void* _write_ptr
    cdef void* _read_ptr
    cdef object _repr
    cdef object file_method
    cdef int flush_writes
    def __init__(self, fn=None, mode=None, read_fd=None, write_fd=None, flush_writes=False):
        self.flush_writes = int(flush_writes)
        cdef char *fnc
        cdef char *modec
        self._repr = "TypedBytesFile(%s, %s, %s, %s)" % (repr(fn), repr(mode), repr(read_fd), repr(write_fd))
        if fn:
            self.file_method = 'fn'
            if mode == None:
                mode = 'r'
            fnc = PyString_AsString(fn)
            modec = PyString_AsString(mode)
            self._write_ptr = self._read_ptr = fopen(fnc, modec)
            if self._write_ptr == NULL:
                raise IOError('Cannot open file [%s]' % fn)
        elif read_fd != None or write_fd != None:
            self.file_method = 'readwritefds'
            self._read_ptr = fdopen(read_fd, 'r') if read_fd != None else <void *>0
            self._write_ptr = fdopen(write_fd, 'w') if write_fd != None else <void *>0
        else:
            self.file_method = 'stdinout'
            self._write_ptr = stdout
            self._read_ptr = stdin

    cdef _close(self):
        self.flush()
        if self.file_method == 'readwritefds':
            if self._write_ptr:
                fclose(self._write_ptr)
            if self._read_ptr:
                fclose(self._read_ptr)
        elif self.file_method == 'stdinout':
            fclose(self._write_ptr)
        self._write_ptr = NULL
        self._read_ptr = NULL

    def __repr__(self):
        return self._repr

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close()

    def __del__(self):
        self._close()

    def __iter__(self):
        return self

    def __next__(self):
        if self._read_ptr == <void *>0:
            raise ValueError("Read pointer not set!")
        return __read_key_value_tb(self._read_ptr)

    def write(self, kv):
        if self._write_ptr == <void *>0:
            raise ValueError("Write pointer not set!")
        __write_key_value_tb(self._write_ptr, kv)
        if self.flush_writes:
            self.flush()

    def writes(self, kvs):
        if self._write_ptr == <void *>0:
            raise ValueError("Write pointer not set!")
        for kv in kvs:
            __write_key_value_tb(self._write_ptr, kv)
        if self.flush_writes:
            self.flush()

    cpdef flush(self):
        if self._write_ptr:
            fflush(self._write_ptr)

    def close(self):
        self._close()
