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

import sys
import os
from operator import itemgetter
from itertools import groupby
import typedbytes
import types
import cPickle as pickle

cdef extern from "stdlib.h":
    void *malloc(size_t size)
    void free(void *ptr)

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, void *stream)
    void *stdin
    void *stdout
    void *stderr
    int getchar()
    size_t fread(void *ptr, size_t size, size_t nmemb, void *stream)
    size_t fwrite(void *ptr, size_t size, size_t nmemb, void *stream)

cdef extern from "endian.h":
    int be32toh(int val)
    long be64toh(long val)
    int htobe32(int val)
    long htobe64(long val)

cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)
    int PyString_AsStringAndSize(object obj, char **buffer, Py_ssize_t *length)
    

# Begin TB
cdef _read_int():
    """Read integer

    Code: 3
    Format: <32-bit signed integer>

    Returns:
        Python int
    """
    cdef int val
    fread(&val, 4, 1, stdin)  # = 1
    return int(be32toh(val))


cdef _raw_write_int(val):
    """Write integer (used for sizes)

    Args:
        val: Python int

    Raises:
        OverflowError: If val overflows an int
    """
    cdef int cval = val
    cval = htobe32(cval)
    fwrite(&cval, 4, 1, stdout)  # = 1


cdef _write_int(val):
    """Write integer

    Code: 3
    Format: <32-bit signed integer>

    Args:
        val: Python int
    """
    cdef int cval
    try:
        cval = val
    except OverflowError:
        return _write_long(val)
    cval = htobe32(cval)
    fwrite(&cval, 4, 1, stdout)  # = 1


def write_int(val):
    _write_int(val)


cdef _read_long():
    """Read integer

    Code: 4
    Format: <64-bit signed integer>

    Returns:
        Python int
    """
    cdef long val
    fread(&val, 8, 1, stdin)  # = 1
    return int(be64toh(val))


cdef _write_long(val):
    """Write long

    Code: 4
    Format: <64-bit signed integer>

    Args:
        val: Python int
    """
    cdef long cval = val
    cval = htobe64(cval)
    fwrite(&cval, 8, 1, stdout)  # = 1


def write_long(val):
    _write_long(val)


cdef _read_float():
    """Read float

    Code: 5
    Format: <32-bit IEEE floating point number>

    Returns:
        Python float
    """
    cdef int val
    fread(&val, 4, 1, stdin)  # = 1
    val = be32toh(val)
    return float((<float*>&val)[0])


cdef _write_float(val):
    """Write float

    Code: 5
    Format: <32-bit IEEE floating point number>

    Args:
        val: Python float
    """
    cdef float cval = val
    cdef int cvalo = htobe32((<int*>&cval)[0])
    fwrite(&cvalo, 4, 1, stdout)  # = 1


def write_float(val):
    _write_float(val)


cdef _read_double():
    """Read double

    Code: 6
    Format: <64-bit IEEE floating point number>

    Returns:
        Python float
    """
    cdef long val
    fread(&val, 8, 1, stdin)  # = 1
    val = be64toh(val)
    return float((<double*>&val)[0])


cdef _write_double(val):
    """Write double

    Code: 6
    Format: <64-bit IEEE floating point number>

    Args:
        val: Python float
    """
    cdef double cval = val
    cdef long cvalo = htobe64((<long*>&cval)[0])
    fwrite(&cvalo, 8, 1, stdout)  # = 1


def write_double(val):
    _write_double(val)


cdef _read_byte():
    """Read byte

    Code: 1
    Format: <signed byte>

    Returns:
        Python int
    """
    cdef signed char val
    fread(&val, 1, 1, stdin)  # = 1
    return int(val)


cdef _write_byte(val):
    """Write byte

    Code: 1
    Format: <signed byte>

    Args:
        val: Python int
    """
    cdef signed char cval = val
    fwrite(&cval, 1, 1, stdout)  # = 1


def write_byte(val):
    _write_byte(val)
    

cdef _read_bool():
    """Read integer

    Code: 2
    Format: <signed byte (0 = false and 1 = true)>

    Returns:
        Python Bool
    """
    return bool(_read_byte())


cdef _write_bool(val):
    """Write bool

    Code: 2
    Format: <signed byte (0 = false and 1 = true)>

    Args:
        val: Python bool
    """
    cdef signed char cval = val
    fwrite(&cval, 1, 1, stdout)  # = 1


def write_bool(val):
    _write_byte(val)


cdef _read_bytes():
    """Read bytes

    Code: 0
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Returns:
        Python string of bytes
    """
    sz = _read_int()
    cdef char *bytes = <char*>malloc(sz)
    fread(bytes, sz, 1, stdin)  # = 1
    out = PyString_FromStringAndSize(bytes, sz)
    free(bytes)
    return out

cdef _write_bytes(val):
    """Write bytes

    Code: 0
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Args:
        val: Python string (str or unicode)
    """
    cdef char *bytes
    cdef Py_ssize_t sz
    PyString_AsStringAndSize(val, &bytes, &sz)  # != -1
    _raw_write_int(sz)
    fwrite(bytes, sz, 1, stdout)  # = 1


def write_bytes(val):
    _write_bytes(val)


cdef _read_string():
    """Read string

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Returns:
        Python string
    """
    return _read_bytes()


cdef _write_string(val):
    """Write string

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Args:
        val: Python string
    """
    _write_bytes(val)


def write_string(val):
    _write_string(val)


cdef _read_unicode():
    """Read unicode

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Returns:
        Python unicode string
    """
    return _read_bytes()


cdef _write_unicode(val):
    """Write unicode

    Code: 7
    Format: <32-bit signed integer> <as many UTF-8 bytes as indicated by the integer>

    Args:
        val: Python string
    """
    _write_bytes(val)


def write_unicode(val):
    _write_unicode(val)


cdef _read_vector():
    """Read fixed length vector of typedbytes

    Code: 8
    Format: <32-bit signed integer> <as many typed bytes sequences as indicated by the integer>

    Returns:
        Python tuple with nested values
    """
    sz = _read_int()
    out = []
    for x in range(sz):
        out.append(_read_tb_code())
    return tuple(out)


cdef _write_vector(val):
    """Write fixed length vector of typedbytes

    Code: 8
    Format: <32-bit signed integer> <as many typed bytes sequences as indicated by the integer>

    Args:
        val: Python tuple with nested values
    """
    cdef int sz = len(val)
    _raw_write_int(sz)
    for x in val:
        _write_tb_code(x)


def write_vector(val):
    _write_vector(val)


cdef _read_list():
    """Read variable length list of typedbytes

    Code: 9
    Format: <variable number of typed bytes sequences> <255 written as an unsigned byte>

    Returns:
        Python list of nested values
    """
    out = []
    while True:
        try:
            out.append(_read_tb_code())
        except StopIteration:
            break
    return out


cdef _write_list(val):
    """Write variable length list of typedbytes

    Code: 9
    Format: <variable number of typed bytes sequences> <255 written as an unsigned byte>

    Args:
        val: Python list of nested values
    """
    for x in val:
        _write_tb_code(x)
    cdef unsigned char code = 255
    fwrite(&code, 1, 1, stdout)  # = 1


def write_list(val):
    _write_list(val)


cdef _read_map():
    """Read fixed length pairs of typedbytes (interpreted as a dict/map)

    Code: 10
    Format: <32-bit signed integer> <as many (key-value) pairs of typed bytes sequences as indicated by the integer>

    Returns:
        Python dict with nested values
    """
    sz = _read_int()
    out = {}
    for x in range(sz):
        k, v = _read_tb_code(), _read_tb_code()
        out[k] = v
    return out


cdef _write_map(val):
    """Write fixed length pairs of typedbytes (interpreted as a dict/map)

    Code: 10
    Format: <32-bit signed integer> <as many (key-value) pairs of typed bytes sequences as indicated by the integer>

    Args:
        val: Python dict with nested values
    """
    _raw_write_int(len(val))
    for x, y in val.iteritems():
        _write_tb_code(x)
        _write_tb_code(y)


def write_map(val):
    _write_map(val)


cdef _read_pickle():
    """Read a python pickle

    Code: 100 (custom)
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Returns:
        Python object
    """
    return pickle.loads(_read_bytes())


cdef _write_pickle(val):
    """Write a python pickle

    Code: 100 (custom)
    Format: <32-bit signed integer> <as many bytes as indicated by the integer>

    Args:
        val: Python object
    """
    _write_bytes(pickle.dumps(val, -1))


def write_pickle(val):
    _write_pickle(val)


# 0: _write_bytes unused
# 1: _write_byte unused
# 5: _write_float unused
_out_types = {types.BooleanType: (2, write_bool),
              types.IntType: (3, write_int),
              types.LongType: (4, write_long),
              types.FloatType: (6, write_double),
              types.StringType: (7, write_string),
              types.UnicodeType: (7, write_unicode),
              types.TupleType: (8, write_vector),
              types.ListType: (9, write_list),
              types.DictType: (10, write_map)}


def _write_tb_code(val):
    cdef int type_code
    try:
        type_code, func = _out_types[type(val)]
    except KeyError:
        type_code, func = 100, write_pickle
    if type_code == 3 and (val < -2147483648 or 2147483647 < val):
        type_code = 4
    if type_code == 4 and (val < -9223372036854775808L or 9223372036854775807L < val):
        type_code, func = 100, write_pickle
    fwrite(&type_code, 1, 1, stdout)  # = 1
    func(val)


def _read_tb_code():
    cdef int type_code = getchar()
    if type_code == 0:
        return _read_bytes()
    elif type_code == 1:
        return _read_byte()
    elif type_code == 2:
        return _read_bool()
    elif type_code == 3:
        return _read_int()
    elif type_code == 4:
        return _read_long()
    elif type_code == 5:
        return _read_float()
    elif type_code == 6:
        return _read_double()
    elif type_code == 7:
        return _read_bytes()
    elif type_code == 8:
        return _read_vector()
    elif type_code == 9:
        return _read_list()
    elif type_code == 10:
        return _read_map()
    elif type_code == 100:
        return _read_pickle()
    elif type_code == 255:
        raise StopIteration
    elif type_code < 0:
        raise StopIteration        
    else:
        raise IndexError('Bad index %d ' % type_code)


cdef __read_key_value_tb():
    k = _read_tb_code()
    v = _read_tb_code()
    return k, v


cdef __write_key_value_tb(kv):
    k, v = kv
    _write_tb_code(k)
    _write_tb_code(v)
    

def read_tb():
    return __read_key_value_tb()


def write_tb(kv):
    __write_key_value_tb(kv)

