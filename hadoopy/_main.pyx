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
import hadoopy

cdef extern from "stdlib.h":
    void *malloc(size_t size)
    void free(void *ptr)


cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, void *stream)
    void *stdin
    void *stdout


cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)


cdef __read_key_value_text():
    cdef ssize_t sz
    cdef char *lineptr = NULL
    cdef size_t n = 0
    sz = getdelim(&lineptr, &n, ord('\t'), stdin)
    if sz == -1:
        raise StopIteration
    k = PyString_FromStringAndSize(lineptr, sz - 1)
    free(lineptr)
    lineptr = NULL
    sz = getdelim(&lineptr, &n, ord('\n'), stdin)
    if sz == -1:
        raise StopIteration
    v = PyString_FromStringAndSize(lineptr, sz - 1)
    free(lineptr)
    return k, v


_line_count = 0
cdef __read_offset_value_text():
    global _line_count
    cdef ssize_t sz
    cdef char *lineptr = NULL
    cdef size_t n = 0
    sz = getdelim(&lineptr, &n, ord('\n'), stdin)
    if sz == -1:
        raise StopIteration
    line = PyString_FromStringAndSize(lineptr, sz - 1)
    free(lineptr)
    out_count = _line_count
    _line_count += sz
    return out_count, line


def _one_offset_value_text():
    return __read_offset_value_text()


def _one_key_value_text():
    return __read_key_value_text()


def _one_key_value_tb():
    return hadoopy.typedbytes.read_tb()


def _print_out_text(iter):
    for k, v in iter:
        print('%s\t%s' % (k, v))


def _print_out_tb(iter):
    for x in iter:
        hadoopy.typedbytes.write_tb(x)


def _print_out(iter):
    """Given an iterator, output the paired values

    Args:
        iter: Iterator of (key, value)
    """
    _print_out_tb(iter) if _is_io_typedbytes() else _print_out_text(iter)


class KeyValueStream(object):
    """Represents KeyValue input streams as iterators

    Supports putting back one item
    """
    def __init__(self, key_value_func):
        """
        Args:
            key_value_func: Function that returns a KeyValue tuple.  Raises
                StopIteration when it is exhausted.
        """
        self._key_value_fun = key_value_func
        self._prev = None
        self._done = False

    def __iter__(self):
        return self

    def next(self):
        """Get next KeyValue tuple

        If a value was replaced using 'put', then it is used and cleared.

        Returns:
            (key, value)

        Raises:
            StopIteration: When key_value_func is exhausted.
        """
        if self._prev:
            prev = self._prev
            self._prev = None
            return prev
        if self._done:
            raise StopIteration
        try:
            return self._key_value_fun()
        except StopIteration, e:
            self._done = True
            raise e

    def put(self, kv):
        """Place an item back into the stream, return following call to 'next'

        Args:
            kv: Previous KeyValue tuple to place back
        """
        self._prev = kv


class GroupedValues(object):
    def __init__(self, group_key, key_value_iter):
        self._key_value_iter = key_value_iter
        self._group_key = group_key
        self._done = False

    def __iter__(self):
        return self

    def next(self):
        if self._done:
            raise StopIteration
        try:
            k, v = self._key_value_iter.next()
        except StopIteration, e:
            self._done = True
            raise e
        # If we get to the end, put the value back
        if k != self._group_key:
            self._done = True
            self._key_value_iter.put((k, v))
            raise StopIteration
        return v


class GroupedKeyValues(object):
    def __init__(self, key_value_iter):
        self._key_value_iter = key_value_iter
        self._prev = None
        self._done = False

    def __iter__(self):
        return self

    def next(self):
        if self._done:
            raise StopIteration
        # Exhaust prev
        if self._prev:
            for x in self._prev:
                pass
        try:
            k, v = self._key_value_iter.next()
        except StopIteration, e:
            self._done = True
            raise e
        self._key_value_iter.put((k, v))
        self._prev = GroupedValues(k, self._key_value_iter)
        return k, self._prev


def _is_io_typedbytes():
    # Only all or nothing typedbytes is supported, just check stream_map_input
    try:
        return os.environ['stream_map_input'] == 'typedbytes'
    except KeyError:
        return False


def _read_in_map():
    """
    Returns:
        Iterator that can be called to get KeyValue pairs.
    """
    if _is_io_typedbytes():
        return KeyValueStream(_one_key_value_tb)
    return KeyValueStream(_one_key_value_text)# was offset


def _read_in_reduce():
    """
    Returns:
        Iterator that can be called to get grouped KeyValues.
    """
    if _is_io_typedbytes():
        return GroupedKeyValues(KeyValueStream(_one_key_value_tb))
    return GroupedKeyValues(KeyValueStream(_one_key_value_text))


def process_inout(work_func, in_iter, out_func, attr):
    if work_func == None:
        return 1
    if isinstance(work_func, type):
        work_func = work_func()
    try:
        work_func.configure()
    except AttributeError:
        pass
    try:
        call_work_func = getattr(work_func, attr)
    except AttributeError:
        call_work_func = work_func
    for x in in_iter:
        work_iter = call_work_func(*x)
        if work_iter != None:
            out_func(work_iter)
    try:
        work_iter = work_func.close()
    except AttributeError:
        pass
    else:
        if work_iter != None:
            out_func(work_iter)
    return 0


def run(mapper=None, reducer=None, combiner=None, **kw):
    if len(sys.argv) >= 2:
        val = sys.argv[1]
        if val == 'map':
            ret = process_inout(mapper, _read_in_map(), _print_out, 'map')
        elif val == 'reduce':
            ret = process_inout(reducer, _read_in_reduce(), _print_out, 'reduce')
        elif val == 'combine':
            ret = process_inout(reducer, _read_in_reduce(), _print_out, 'reduce')
        elif val == 'freeze' and len(sys.argv) > 2:
            extra_files = []
            pos = 3
            # Any file with -Z is added to the tar
            while len(sys.argv) >= pos + 2 and sys.argv[pos] == '-Z':
                extra_files.append(sys.argv[pos + 1])
                pos += 2
            extra = ' '.join(sys.argv[pos:])
            hadoopy._freeze.freeze_to_tar(script_path=sys.argv[0],
                                          freeze_fn=sys.argv[2], extra=extra,
                                          extra_files=extra_files)
        else:
            print_doc_quit(kw['doc'])
    else:
        ret = 1
    if ret and 'doc' in kw:
        print_doc_quit(kw['doc'])
    return ret


def print_doc_quit(doc):
    print(doc)
    sys.exit(1)
