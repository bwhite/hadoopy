#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Hadoopy Wordcount Demo
"""

__author__ =  'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

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
