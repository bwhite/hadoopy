import sys


def parse_args(mapper, reducer=None, combiner=None, separator='\t'):
    if len(sys.argv) < 2:
        return 1
    method = sys.argv[1]
    if method == 'map':
        for key, val in mapper():
            print(''.join((str(key), separator, str(val))))
        return 0
    elif method == 'reduce':
        for key, val in reducer():
            print(''.join((str(key), separator, str(val))))
        return 0
    elif method == 'combine':
        for key, val in combiner():
            print(''.join((str(key), separator, str(val))))
        return 0
    return 1
