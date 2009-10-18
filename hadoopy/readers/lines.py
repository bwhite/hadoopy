#!/usr/bin/env python
import sys

def read_lines():
    for line in sys.stdin:
        line = line.rstrip()
        if line:
            yield line
