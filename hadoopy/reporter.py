import sys


def _err(x):
    sys.stderr.write(x)


def counter(group, counter, amount=1, err=_err):
    err("reporter:counter:%s,%s,%s\n" % (group, counter, str(amount)))


def status(msg, err=_err):
    err(''.join(("reporter:status:", str(msg))))
