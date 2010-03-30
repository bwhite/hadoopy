import sys
from operator import itemgetter
from itertools import groupby


def _print_out(out, separator):
    if isinstance(out, tuple):
        print(separator.join(str(x) for x in out))
    else:
        print(str(out))


def _key_values(separator='\t'):
    for line in sys.stdin:
        yield line.rstrip().split(separator, 1)


def _groupby_key_values(separator='\t'):
    return ((x, (z[1] for z in y))
            for x, y in groupby(_key_values(separator), itemgetter(0)))


def _offset_values():
    line_count = 0
    for line in sys.stdin:
        line = line[:-1]
        yield line_count, line
        line_count += 1


def _handle_final(func, separator):
    for out in func():
        if out:
            _print_out(out, separator)
    return 0


def configure_call_close(attr):
    def factory(f):
        def inner(func, separator):
            try:
                func.configure()
            except AttributeError:
                pass
            try:
                return f(getattr(func, attr), separator)
            except AttributeError:
                return f(func, separator)
            finally:
                try:
                    _handle_final(func.close, separator)
                except AttributeError:
                    pass
        return inner
    return factory


@configure_and_close('map')
def _handle_map(func, separator):
    for key, value in _offset_values():
        for out in func(key, value):
            if out:
                _print_out(out, separator)
    return 0


@configure_and_close('reduce')
def _handle_reduce(func, separator):
    for key, values in _groupby_key_values(separator):
        for out in func(key, values):
            if out:
                _print_out(out, separator)
    return 0


def run(mapper=None, reducer=None, combiner=None, separator='\t'):
    if len(sys.argv) < 2:
        return 1
    method = sys.argv[1]
    if method == 'map':
        return _handle_map(mapper, separator)
    elif method == 'reduce':
        return _handle_reduce(reducer, separator)
    elif method == 'combine':
        return _handle_reduce(combiner, separator)
    return 1
