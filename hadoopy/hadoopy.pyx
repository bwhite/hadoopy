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


#def _key_values_text(sep='\t'):
#    for line in sys.stdin:
#        yield line.rstrip().split(sep, 1)


#def _key_values_tb():
#    return typedbytes.PairedInput(sys.stdin).reads()




# These are converted such that they are called for each key/value pair
def _one_key_values_text():
    return sys.stdin.readline()[:-1].split('\t', 1)

def _one_key_values_tb():
    # TODO
    return typedbytes.PairedInput(sys.stdin).reads()


_key_values_text_group
def _key_values_text_group():
    
    

#def _groupby_kv(kv):
#    return ((x, (z[1] for z in y))
#            for x, y in groupby(kv, itemgetter(0)))


#def _offset_values_text():
#    line_count = 0
#    for line in sys.stdin:
#        yield line_count, line[:-1]
#        line_count += len(line)

_line_count = 0
def _one_offset_values_text():
    global _line_count
    out_count = _line_count
    line = sys.stdin.readline()
    _line_count += len(line)
    return out_count, line[:-1]


def _is_io_typedbytes():
    # Only all or nothing typedbytes is supported, just check stream_map_input
    try:
        return os.environ['stream_map_input'] == 'typedbytes'
    except KeyError:
        return False


def _read_in_map():
    if _is_io_typedbytes():
        return _one_key_value_tb
    return _one_offset_value_text


def _read_in_reduce():
    """
    Returns:
        Function that can be called to receive grouped input.  Function returns
        None when there is no more input.
    """
    if _is_io_typedbytes():
        return _key_values_tb_group
    return _key_values_text_group


def _print_out_text(iter, sep='\t'):
    for out in iter:
        if isinstance(out, tuple):
            print(sep.join(str(x) for x in out))
        else:
            print(str(out))


def _print_out_tb(iter):
    typedbytes.PairedOutput(sys.stdout).writes(iter)


def _print_out(iter):
    """Given an iterator, output the paired values

    Args:
        iter: Iterator of (key, value)
    """
    _print_out_tb(iter) if _is_io_typedbytes() else _print_out_text(iter)


def process_inout(work_func, in_func, out_func, attr):
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
    while 1:
        x = in_func()
        if x == None:
            break
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


def _map(func):
    return process_inout(func, _read_in_map(), _print_out, 'map')


def _reduce(func):
    return process_inout(func, _groupby_kv(_read_in_reduce()), _print_out, 'reduce')


def run(mapper=None, reducer=None, combiner=None, **kw):
    if len(sys.argv) >= 2:
        val = sys.argv[1]
        if val == 'map':
            _map(mapper)
        elif val == 'reduce':
            _reduce(reducer)
        elif val == 'combine':
            _reduce(reducer)
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
