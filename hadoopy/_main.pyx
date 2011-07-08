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
import subprocess
import json

def change_dir():
    # Skip this process if the command used is pipe.  The child will still get
    # the environmental variable
    if len(sys.argv) >= 2 and sys.argv[1] == 'pipe':
        return
    try:
        d = os.environ['HADOOPY_CHDIR']
        sys.stderr.write('HADOOPY: Trying to chdir to [%s]\n' % d)
    except KeyError:
        pass
    else:
        try:
            os.chdir(d)
        except OSError:
            sys.stderr.write('HADOOPY: Failed to chdir to [%s]\n' % d)

# This is called immediately so that client imports are in their expected dir
change_dir()


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
    cdef object args
    cdef int read_fd
    cdef int write_fd
    cdef void* read_fp
    cdef object tb

    def __init__(self, mapper, reducer, combiner, task_type, *args, **kw):
        self.mapper = mapper
        self.reducer = reducer
        self.combiner = combiner
        self.task_type = task_type
        self.line_count = 0
        try:
            self.read_fd = int(args[0])
        except IndexError:
            self.read_fd = sys.stdin.fileno()
        try:
            self.write_fd = int(args[1])
        except IndexError:
            self.write_fd = sys.stdout.fileno()
        self.read_fp = fdopen(self.read_fd, 'r')
        self.tb = hadoopy.TypedBytesFile(read_fd=self.read_fd, write_fd=self.write_fd)

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


def freeze():
    extra_files = []
    pos = 3
    # Any file with -Z is added to the tar
    while len(sys.argv) >= pos + 2 and sys.argv[pos] == '-Z':
        extra_files.append(sys.argv[pos + 1])
        pos += 2
    extra = ' '.join(sys.argv[pos:])
    hadoopy._freeze.freeze_to_tar(script_path=os.path.abspath(sys.argv[0]),
                                  freeze_fn=sys.argv[2],
                                  extra_files=extra_files)


def run(mapper=None, reducer=None, combiner=None, **kw):
    """Hadoopy entrance function

    This is to be called in all Hadoopy job's.  Handles arguments passed in,
    calls the provided functions with input, and stores the output.

    TypedBytes are used if the following is True
    os.environ['stream_map_input'] == 'typedbytes'

    It is *highly* recommended that TypedBytes be used for all non-trivial
    tasks.  Keep in mind that the semantics of what you can safely emit from
    your functions is limited when using Text (i.e., no \\t or \\n).  You can use
    the base64 module to ensure that your output is clean.

    If the HADOOPY_CHDIR environmental variable is set, this will immediately
    change the working directory to the one specified.  This is useful if your
    data is provided in an archive but your program assumes it is in that
    directory.

    As hadoop streaming relies on stdin/stdout/stderr for communication,
    anything that outputs on them in an unexpected way (especially stdout) will
    break the pipe on the Java side and can potentially cause data errors.  To
    fix this problem, hadoopy allows file descriptors (integers) to be provided
    to each task.  These will be used instead of stdin/stdout by hadoopy.  This
    is designed to combine with the 'pipe' command.

    To use the pipe functionality, instead of using
    `your_script.py map` use `your_script.py pipe map`
    which will call the script as a subprocess and use the read_fd/write_fd
    command line arguments for communication.  This isolates your script and
    eliminates the largest source of errors when using hadoop streaming.

    The pipe functionality has the following semantics
    stdin: Always an empty file
    stdout: Redirected to stderr (which is visible in the hadoop log)
    stderr: Kept as stderr
    read_fd: File descriptor that points to the true stdin
    write_fd: File descriptor that points to the true stdout

    | **Command Interface**
    | The command line switches added to your script (e.g., script.py) are

    python script.py *map* (read_fd) (write_fd)
        Use the provided mapper, optional read_fd/write_fd.
    python script.py *reduce* (read_fd) (write_fd)
        Use the provided reducer, optional read_fd/write_fd.
    python script.py *combine* (read_fd) (write_fd)
        Use the provided combiner, optional read_fd/write_fd.
    python script.py *freeze* <tar_path> <-Z add_file0 -Z add_file1...>
        Freeze the script to a tar file specified by <tar_path>.  The extension
        may be .tar or .tar.gz.  All files are placed in the root of the tar.
        Files specified with -Z will be added to the tar root.
    python script.py info
        Prints a json object containing 'tasks' which is a list of tasks which
        can include 'map', 'combine', and 'reduce'.  Also contains 'doc' which is
        the provided documentation through the doc argument to the run function.
        The tasks correspond to provided inputs to the run function.

    | **Specification of mapper/reducer/combiner** 
    | Input Key/Value Types
    |     For TypedBytes/SequenceFileInputFormat, the Key/Value are the decoded TypedBytes
    |     For TextInputFormat, the Key is a byte offset (int) and the Value is a line without the newline (string)
    |
    | Output Key/Value Types
    |     For TypedBytes, anything Pickle-able can be used
    |     For Text, types are converted to string.  Note that neither may contain \\t or \\n as these are used in the encoding.  Output is key\\tvalue\\n
    |
    | Expected arguments
    |     mapper(key, value) or mapper.map(key, value)
    |     reducer(key, values) or reducer.reduce(key, values)
    |     combiner(key, values) or combiner.reduce(key, values)
    |
    | Optional methods
    |     func.configure(): Called before any input read.  Returns None.
    |     func.close():  Called after all input read.  Returns None or Iterator of (key, value)
    |
    | Expected return
    |     None or Iterator of (key, value)

    :param mapper: Function or class following the above spec
    :param reducer: Function or class following the above spec
    :param combiner: Function or class following the above spec
    :param doc: If specified, on error print this and call sys.exit(1)
    :rtype: True on error, else False (may not return if doc is set and
        there is an error)
    """
    ret = 0
    if len(sys.argv) >= 2:
        if sys.argv[1] == 'freeze':
            if len(sys.argv) > 2:
                ret = freeze()
            else:
                print('Usage: python script.py freeze <tar_path> <-Z add_file0 -Z add_file1...>')
        elif sys.argv[1] == 'pipe' and len(sys.argv) == 3:
            cmd = '%s %s %d %d' % (sys.argv[0],
                                   sys.argv[2],
                                   os.dup(sys.stdin.fileno()),
                                   os.dup(sys.stdout.fileno()))
            slave_stdin = open('/dev/null', 'r')
            slave_stdout = os.fdopen(os.dup(sys.stderr.fileno()), 'w')
            retcode = 0
            try:
                retcode = subprocess.call(cmd.split(), stdout=slave_stdout, stdin=slave_stdin)
            except OSError:  # If we can't find the file, check the local dir
                retcode = subprocess.call(('./' + cmd).split(), stdout=slave_stdout, stdin=slave_stdin)
            if retcode:
                sys.exit(retcode)
        elif sys.argv[1] == 'info':
            tasks = []
            if mapper:
                tasks.append('map')
            if reducer:
                tasks.append('reduce')
            if combiner:
                tasks.append('combine')
            info = dict(kw)
            info['tasks'] = tasks
            print(json.dumps(info))
        elif sys.argv[1] in ['map', 'reduce', 'combine']:
            ret = HadoopyTask(mapper, reducer, combiner, *sys.argv[1:]).run()
        else:
            ret = 1
    else:
        ret = 1
    if ret and 'doc' in kw:
        print_doc_quit(kw['doc'])
    return bool(ret)


def print_doc_quit(doc):
    print(doc)
    sys.exit(1)
