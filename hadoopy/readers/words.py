#!/usr/bin/env python
import re
import sys

def read_words():
    """Reads stdin, returns an iterator providing each word in the file (split by whitespace)."""
    word_finditer = re.compile('(\w+)').finditer
    for line in sys.stdin:
        for re_group in word_finditer(line):
            yield re_group.group(1)
