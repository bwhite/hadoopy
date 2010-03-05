import sys
import itertools
import operator


def _print_out(out, separator):
    if isinstance(out, tuple):
        print(separator.join(str(x) for x in out))
    else:
        print(str(out))


def _key_values(separator='\t'):
    for line in sys.stdin:
        yield line.rstrip().split(separator, 1)


def _groupby_key_values(separator='\t'):
    return ((x, (z[1] for z in y)) for x, y in itertools.groupby(_key_values(separator), operator.itemgetter(0)))


def _offset_values():
    line_count = 0
    for line in sys.stdin:
        line = line[:-1]
        yield line_count, line
        line_count += 1


def _handle_map(mapper, separator):
    for key, value in _offset_values():
        for out in mapper(key, value):
            if out:
                _print_out(out, separator)
    return 0


def _handle_reduce(reducer, separator):
    for key, values in _groupby_key_values(separator):
        for out in reducer(key, values):
            if out:
                _print_out(out, separator)
    return 0


def _handle_final(func, separator):
    for out in func():
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


def final(mapper=None, reducer=None, combiner=None, separator='\t'):
    """Used to call provided functions when there is no more input.
    This is used for patterns such as in-map-combiner where you need
    a stable destructor to call at the end to flush the buffered data.
    """
    if len(sys.argv) < 2:
        return 1
    method = sys.argv[1]
    if method == 'map':
        return _handle_final(mapper, separator)
    elif method == 'reduce':
        return _handle_final(reducer, separator)
    elif method == 'combine':
        return _handle_final(combiner, separator)
    return 1

def initial(mapper=None, reducer=None, combiner=None):
    """Used to call the proper function depending on arguments"""
    # TODO Should throw exception
    if len(sys.argv) < 2:
        return
    method = sys.argv[1]
    if method == 'map':
        return mapper()
    elif method == 'reduce':
        return reducer()
    elif method == 'combine':
        return combiner()
