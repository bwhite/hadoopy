import sys


def _err(x):
    sys.stderr.write(x)


def counter(group, counter, amount=1):
    _err("reporter:counter:%s,%s,%s\n" % (group, counter, str(amount)))


def status(msg):
    _err(''.join(("reporter:status:", str(msg))))
