import sys
from operator import itemgetter
from itertools import groupby


def _print_out(out, sep):
    if isinstance(out, tuple):
        print(sep.join(str(x) for x in out))
    else:
        print(str(out))


def _key_values(sep='\t'):
    for line in sys.stdin:
        yield line.rstrip().split(sep, 1)


def _groupby_key_values(sep='\t'):
    return ((x, (z[1] for z in y))
            for x, y in groupby(_key_values(sep), itemgetter(0)))


def _offset_values():
    line_count = 0
    for line in sys.stdin:
        line = line[:-1]
        yield line_count, line
        line_count += 1


def _final(func, sep):
    for out in func():
        if out:
            _print_out(out, sep)
    return 0


def _configure_call_close(attr):
    def factory(f):
        def inner(func, sep):
            try:
                func.configure()
            except AttributeError:
                pass
            try:
                return f(getattr(func, attr), sep)
            except AttributeError:
                return f(func, sep)
            finally:
                try:
                    _final(func.close, sep)
                except AttributeError:
                    pass
        return inner
    return factory


@_configure_and_close('map')
def _map(func, sep):
    for key, value in _offset_values():
        for out in func(key, value):
            if out:
                _print_out(out, sep)
    return 0


@_configure_and_close('reduce')
def _reduce(func, sep):
    for key, values in _groupby_key_values(sep):
        for out in func(key, values):
            if out:
                _print_out(out, sep)
    return 0


def run(mapper=None, reducer=None, combiner=None, sep='\t'):
    funcs = {'map': lambda: _map(mapper, sep),
             'reduce': lambda: _reduce(reducer, sep),
             'combine': lambda: _reduce(combiner, sep)}
    try:
        return funcs[sys.argv[1]]()
    except (IndexError, KeyError):
        return 1
