#!/usr/bin/env python
import sys

def read_lines():
    """Reads stdin, returns an iterator providing each word in the file (split by whitespace)."""
    for line in sys.stdin:
        line = line.rstrip()
        if line:
            yield line
