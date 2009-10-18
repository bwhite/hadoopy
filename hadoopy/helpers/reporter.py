import sys

def counter(group, counter, amount=1):
    sys.stderr.write("reporter:counter:%s,%s,%s\n"%(group, counter, str(amount)))

def status(msg):
    sys.stderr.write(''.join(("reporter:status:", msg)))
