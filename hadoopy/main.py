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


def _key_values_text(sep='\t'):
    for line in sys.stdin:
        yield line.rstrip().split(sep, 1)


def _key_values_tb():
    return typedbytes.PairedInput(sys.stdin).reads()


def _groupby_kv(kv):
    return ((x, (z[1] for z in y))
            for x, y in groupby(kv, itemgetter(0)))


def _offset_values_text():
    line_count = 0
    for line in sys.stdin:
        yield line_count, line[:-1]
        line_count += len(line)


def _is_io_typedbytes():
    # Only all or nothing typedbytes is supported, just check stream_map_input
    try:
        return os.environ['stream_map_input'] == 'typedbytes'
    except KeyError:
        return False


def _read_in_map():
    if _is_io_typedbytes():
        return _key_values_tb()
    return _offset_values_text()


def _read_in_reduce():
    if _is_io_typedbytes():
        return _key_values_tb()
    return _key_values_text()


def _print_out_text(iter, sep='\t'):
    for out in iter:
        if isinstance(out, tuple):
            print(sep.join(str(x) for x in out))
        else:
            print(str(out))


def _print_out_tb(iter):
    typedbytes.PairedOutput(sys.stdout).writes(iter)


def _print_out(iter):
    _print_out_tb(iter) if _is_io_typedbytes() else _print_out_text(iter)


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


def _map(func):
    return process_inout(func, _read_in_map(), _print_out, 'map')


def _reduce(func):
    return process_inout(func, _groupby_kv(_read_in_reduce()), _print_out, 'reduce')


def run(mapper=None, reducer=None, combiner=None, **kw):
    funcs = {'map': lambda: _map(mapper),
             'reduce': lambda: _reduce(reducer),
             'combine': lambda: _reduce(combiner)}
    try:
        ret = funcs[sys.argv[1]]()
    except (IndexError, KeyError):
        ret = 1
    if ret and 'doc' in kw:
        print_doc_quit(kw['doc'])
    return ret


def print_doc_quit(doc):
    print(doc)
    sys.exit(1)
