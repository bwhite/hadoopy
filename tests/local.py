#!/usr/bin/env python
import hadoopy
import sys
import os

PRINTED = False
def mapper(k, v):
    global PRINTED, COUNTER
    if not PRINTED and os.environ['TEST_ENV']:
        print('Loud and clear[%s]' % os.environ['TEST_ENV'])
        with open('test_file') as fp:
            print(fp.read())
        PRINTED = True
    yield k, 1


def reducer(k, vs):
    yield k, sum(vs)


if __name__ == '__main__':
    hadoopy.run(mapper, reducer, reducer)
