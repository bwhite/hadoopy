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
import hadoopy._runner


cdef extern from "stdlib.h":
    void *malloc(size_t size)
    void free(void *ptr)


cdef extern from "getdelim.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, void *stream)

cdef extern from "stdio.h":
    #ssize_t getdelim(char **lineptr, size_t *n, int delim, void *stream)
    void *fdopen(int fd, char *mode)
    int fflush(void *stream)
    void *stdin
    void *stdout


cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *s, Py_ssize_t len)


cdef class KeyValueStream(object):
    """Represents KeyValue input streams as iterators

    Supports putting back one item
    """
    cdef object _key_value_func
    cdef object _prev
    cdef object _done
    def __init__(self, key_value_func):
        """
        Args:
            key_value_func: Function that returns a KeyValue tuple.  Raises
                StopIteration when it is exhausted.
        """
        self._key_value_func = key_value_func
        self._prev = None
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
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
            return self._key_value_func()
        except StopIteration, e:
            self._done = True
            raise e

    cpdef object put(self, kv):
        """Place an item back into the stream, return following call to 'next'

        Args:
            kv: Previous KeyValue tuple to place back
        """
        self._prev = kv


cdef class GroupedValues(object):
    cdef object _key_value_iter
    cdef object _group_key
    cdef object _done
    
    def __init__(self, group_key, key_value_iter):
        self._key_value_iter = key_value_iter
        self._group_key = group_key
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
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


cdef class GroupedKeyValues(object):
    cdef object _key_value_iter
    cdef object _prev
    cdef object _done
    def __init__(self, key_value_iter):
        self._key_value_iter = key_value_iter
        self._prev = None
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
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


cdef class HadoopyTask(object):
    cdef object mapper
    cdef object reducer
    cdef object combiner
    cdef object task_type
    cdef int line_count
    cdef int read_fd
    cdef int write_fd
    cdef void* read_fp
    cdef object tb

    def __init__(self, mapper, reducer, combiner, task_type, read_fd=None, write_fd=None):
        self.mapper = mapper
        self.reducer = reducer
        self.combiner = combiner
        self.task_type = task_type
        self.line_count = 0
        if read_fd is None:
            self.read_fd = sys.stdin.fileno()
        else:
            self.read_fd = int(read_fd)
        if write_fd is None:
            self.write_fd = sys.stdout.fileno()
        else:
            self.write_fd = int(write_fd)
        self.read_fp = fdopen(self.read_fd, 'r')
        self.tb = hadoopy.TypedBytesFile(read_fd=self.read_fd, write_fd=self.write_fd, flush_writes=self.flush_tb_writes())

    # Core methods
    def run(self):
        if self.task_type == 'map':
            return self.process_inout(self.mapper, self.read_in_map(), self.print_out, 'map')
        elif self.task_type == 'reduce':
            return self.process_inout(self.reducer, self.read_in_reduce(), self.print_out, 'reduce')
        elif self.task_type == 'combine':
            return self.process_inout(self.combiner, self.read_in_reduce(), self.print_out, 'reduce')
        else:
            return 1

    @classmethod
    def process_inout(cls, work_func, in_iter, out_func, attr):
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

    # Output methods
    def print_out_text(self, iter):
        for k, v in iter:
            os.write(self.write_fd, '%s\t%s\n' % (k, v))

    def print_out_tb(self, iter):
        self.tb.writes(iter)

    def print_out(self, iter):
        """Given an iterator, output the paired values

        Args:
            iter: Iterator of (key, value)
        """
        self.print_out_tb(iter) if self.is_io_typedbytes() else self.print_out_text(iter)

    # Input methods
    cpdef read_key_value_text(self):
        cdef ssize_t sz
        cdef char *lineptr = NULL
        cdef size_t n = 0
        sz = getdelim(&lineptr, &n, 9, self.read_fp)  # 9 == ord('\t')
        if sz == -1:
            raise StopIteration
        k = PyString_FromStringAndSize(lineptr, sz - 1)
        free(lineptr)
        lineptr = NULL
        sz = getdelim(&lineptr, &n, 10, self.read_fp)  # 10 == ord('\n')
        if sz == -1:
            raise StopIteration
        v = PyString_FromStringAndSize(lineptr, sz - 1)
        free(lineptr)
        return k, v

    cpdef read_offset_value_text(self):
        cdef ssize_t sz
        cdef char *lineptr = NULL
        cdef size_t n = 0
        sz = getdelim(&lineptr, &n, 10, self.read_fp)  # 10 == ord('\n')
        if sz == -1:
            raise StopIteration
        line = PyString_FromStringAndSize(lineptr, sz - 1)
        free(lineptr)
        out_count = self.line_count
        self.line_count += sz
        return out_count, line

    def read_in_map(self):
        """Provides the input iterator to use

        If is_io_typedbytes() is true, then use TypedBytes.
        If is_on_hadoop() is true, then use Text as key\\tvalue\\n.
        Else, then use Text with key as byte offset and value as line (no \\n)

        Returns:
            Iterator that can be called to get KeyValue pairs.
        """
        if self.is_io_typedbytes():
            return KeyValueStream(self.tb.__next__)
        if self.is_on_hadoop():
            return KeyValueStream(self.read_key_value_text)
        return KeyValueStream(self.read_offset_value_text)

    def read_in_reduce(self):
        """
        Returns:
            Iterator that can be called to get grouped KeyValues.
        """
        if self.is_io_typedbytes():
            return GroupedKeyValues(KeyValueStream(self.tb.__next__))
        return GroupedKeyValues(KeyValueStream(self.read_key_value_text))

    # Environment info methods
    def is_io_typedbytes(self):
        # Only all or nothing typedbytes is supported, just check stream_map_input
        try:
            return os.environ['stream_map_input'] == 'typedbytes'
        except KeyError:
            return False

    def is_on_hadoop(self):
        return 'mapred_input_format_class' in os.environ

    def flush_tb_writes(self):
        return 'hadoopy_flush_tb_writes' in os.environ
