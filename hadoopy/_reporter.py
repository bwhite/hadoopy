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

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import sys


def _err(x):
    sys.stderr.write(x)


def counter(group, counter, amount=1, err=None):
    """Output a counter update that is displayed in the Hadoop web interface

    Counters are useful for quickly identifying the number of times an error
    occurred, current progress, or coarse statistics.

    :param group: Counter group
    :param counter: Counter name
    :param amount: Value to add (default 1)
    :param err: Func that outputs a string, if None then sys.stderr.write is used (default None)
    """
    if not err:
        err = _err
    err("reporter:counter:%s,%s,%s\n" % (group, counter, str(amount)))


def status(msg, err=None):
    """Output a status message that is displayed in the Hadoop web interface

    The status message will replace any other, if you want to append you must
    do this yourself.

    :param msg: String representing the status message
    :param err: Func that outputs a string, if None then sys.stderr.write is used (default None)
    """
    if not err:
        err = _err
    err("reporter:status:%s\n" % str(msg))
