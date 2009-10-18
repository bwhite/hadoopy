import sys
import operator
import itertools

def key_values(separator='\t'):
    for line in sys.stdin:
        out = line.rstrip().split(separator, 1)
        if out: # Ignore blanks
            yield out

def groupby_key_values(separator='\t'):
    return itertools.groupby(key_values(separator), operator.itemgetter(0))
