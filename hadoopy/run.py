import sys
from operator import itemgetter
from itertools import groupby
import typedbytes
import StringIO


def _key_values():
    for x in typedbytes.PairedInput(sys.stdin):
        yield x


def _groupby_key_values():
    return ((x, (z[1] for z in y))
            for x, y in groupby(_key_values(), itemgetter(0)))    


def _offset_values():
    line_count = 0
    for line in sys.stdin:
        line = line[:-1]
        yield line_count, line
        line_count += 1


def _print_out(iter):
    out = StringIO.StringIO('w')
    typedbytes.PairedOutput(out).writes(iter)
    out.seek(0)
    print(out.read())

def _final(func, sep):
    _print_out(func())
    return 0


def _configure_call_close(attr):
    def factory(f):
        def inner(func, sep):
            if func == None:
                return 1
            if isinstance(func, type):
                func = func()
            try:
                func.configure()
            except AttributeError:
                pass
            try:
                try:
                    return f(getattr(func, attr), sep)
                except AttributeError:
                    return f(func, sep)
            except ValueError: # func not generator, its ok
                return 0
            finally:
                try:
                    _final(func.close, sep)
                except AttributeError:
                    pass
        return inner
    return factory


@_configure_call_close('map')
def _map(func, sep):
    for key, value in _offset_values():
        _print_out(func(key, value))
    return 0


@_configure_call_close('reduce')
def _reduce(func, sep):
    for key, values in _groupby_key_values():
        _print_out(func(key, values))
    return 0


def run(mapper=None, reducer=None, combiner=None, sep='\t'):
    funcs = {'map': lambda: _map(mapper, sep),
             'reduce': lambda: _reduce(reducer, sep),
             'combine': lambda: _reduce(combiner, sep)}
    try:
        return funcs[sys.argv[1]]()
    except (IndexError, KeyError):
        return 1

def print_doc_quit(doc):
    print(doc)
    sys.exit(1)
