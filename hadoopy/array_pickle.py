#!/usr/bin/env python
"""
This is a module used to efficiently encode a list of string encodings of array
data.  The result is concatenated strings with the number of strings encoded as
a base64 of a 32bit uint.  The list of string encodings will simply be
concatenated, if you need base64 than perform it outside of this module.

Decoded format
[array0_str_encoding, array1_str_encoding, ... , arrayN_str_encoding]
Encoded format
len(base64_of_unsigned_int_length) is always 8
concatenated_strs + base64_of_unsigned_int_length
"""
import numpy as np
import base64


def dumps2d64(vecs):
    """Takes in a list of equal length string encodings of array data.  It
    concatenates them all together and puts the length at the end in the form
    of a base64 encoded unsigned int."""
    vecs_len = np.array(len(vecs), dtype=np.uint).tostring()
    return ''.join(vecs) + base64.b64encode(vecs_len)


def loads2d64(vecs64):
    l = len2d64(vecs64)
    # Fast check for the common case
    if l == 1:
        return (vecs64[:-8],)
    try:
        sz = (len(vecs64) - 8) / l
    except ZeroDivisionError:
        return ()
    return (vecs64[sz * i:sz * (i + 1)] for i in range(l))


def len2d64(vecs64):
    len64 = vecs64[-8:]
    # Fast check for the common case
    if len64 == 'AQAAAA==':
        return 1
    return int(np.fromstring(base64.b64decode(len64), dtype=np.uint)[0])

if __name__ == '__main__':
    l = 5
    r = 10000
    import time
    st = time.time()
    all_correct = True
    for y in range(r):
        for n in range(0, 3):
            a = [np.array(np.random.random(l), dtype=np.float32)
                 for x in range(n)]
            b = [base64.b64encode(x.tostring()) for x in a]
            c = [np.fromstring(base64.b64decode(x), dtype=np.float32)
                 for x in loads2d64(dumps2d64(b))]
            all_correct = all_correct and (np.array(a) == np.array(c)).all()
    print(all_correct)
    print(time.time() - st)
