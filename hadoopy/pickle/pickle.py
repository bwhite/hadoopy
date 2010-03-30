#!/usr/bin/env python
import cPickle as pickle
import base64
import zlib


def dumps64(obj):
    return base64.b64encode(pickle.dumps(obj, 2))


def dumpx64(obj):
    return base64.b64encode(zlib.compress(pickle.dumps(obj, 2)))


def b64enc(obj):
    return base64.b64encode(obj)


def loads64(obj):
    return pickle.loads(base64.b64decode(obj))


def loadx64(obj):
    return pickle.loads(zlib.decompress(base64.b64decode(obj)))


def b64dec(obj):
    return base64.b64decode(obj)
