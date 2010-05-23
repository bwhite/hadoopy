#!/usr/bin/env python
"""Hadoopy Wordcount Demo
"""
import hadoopy


def mapper(key, value):
    """Take in a byte offset and a document, emit word counts.

    Args:
        key: byte offset
        value: document as a string

    Yields:
        A tuple in the form of (key, value)
        key: word (string)
        value: count (int)
    """
    for word in value.split():
        yield word, 1


def reducer(key, values):
    """Take in an iterator of counts for a word, sum them, and return the sum.

    Args:
        key: word (string)
        values: counts (int)

    Yields:
        A tuple in the form of (key, value)
        key: word (string)
        value: count (int)
    """
    accum = 0
    for count in values:
        accum += int(count)
    yield key, accum


if __name__ == "__main__":
    if hadoopy.run(mapper, reducer):
        hadoopy.print_doc_quit(__doc__)
