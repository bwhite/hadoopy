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

import subprocess
import tempfile
import re
import multiprocessing
import hadoopy
import os
import cPickle as pickle

from hadoopy._runner import _find_hstreaming


def ls(path):
    """List files on HDFS.

    Args:
        path: A string (potentially with wildcards).

    Returns:
        A list of strings representing HDFS paths.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    out, err = subprocess.Popen('hadoop fs -ls %s' % path, env={}, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE).communicate()
    if err:
        raise IOError('Ran[%s]: %s' % (path, err))
    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    out = [x.split(' ')[-1] for x in out.split('\n')
           if x and not found_line(x)]
    return out


def _hdfs_cat_tb(args):
    path, hstreaming, fn = args
    script_path = '%s/_hdfs.py' % os.path.dirname(hadoopy.__file__)
    with open(fn, 'wb') as fp:
        subprocess.Popen('hadoop jar %s dumptb %s' % (hstreaming, path),
                         stdout=fp, stderr=subprocess.PIPE,
                         env={}, shell=True).wait()


def cat(path, procs=10):
    """Read typedbytes sequence files on HDFS (with optional compression).

    Args:
        path: A string (potentially with wildcards).
        procs: Number of processes to use.

    Returns:
        An iterator of key, value pairs.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    max_files = 100
    hstreaming = _find_hstreaming()
    all_paths = ls(path)
    p = multiprocessing.Pool(min((procs, max_files, len(all_paths))))
    while all_paths:
        paths = all_paths[:max_files]
        del all_paths[:max_files]
        fps = [tempfile.NamedTemporaryFile() for x in paths]
        p.map(_hdfs_cat_tb, [(path, hstreaming, fp.name)
                             for path, fp in zip(paths, fps)])
        for y in fps:
            for x in hadoopy.TypedBytesFile(y.name, 'r'):
                yield x
