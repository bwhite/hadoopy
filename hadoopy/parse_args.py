import sys

def _print_out(out, separator):
    if isinstance(out, tuple):
        print(separator.join(str(x) for x in out))
    else:
        print(str(out))

def parse_args(mapper, reducer=None, combiner=None, separator='\t'):
    if len(sys.argv) < 2:
        return 1
    method = sys.argv[1]
    if method == 'map':
        for out in mapper():
            _print_out(out, separator)
        return 0
    elif method == 'reduce':
        for out in reducer():
            _print_out(out, separator)
        return 0
    elif method == 'combine':
        for out in combiner():
            _print_out(out, separator)
        return 0
    return 1
