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

import subprocess
import tempfile
import re
import multiprocessing
import hadoopy
import os
import cPickle as pickle

from hadoopy._runner import _find_hstreaming

def _cleaned_hadoop_stderr(hdfs_stderr):
    for line in hdfs_stderr:
        parts = line.split()
        if parts[2] == 'INFO' or parts[2] == 'WARN':
            pass
        else:
            yield line
        

def _checked_hadoop_fs_command(cmd):
    if sys.version[:3] == '2.4':
        return os.system(cmd) / 256
    proc = subprocess.Popen(cmd, env={}, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    out, err = proc.communicate()
    rcode = proc.returncode
    
    return rcode, out, err

def exists(path):
    """Check if a file exists.
    
    Args:
        path: A string for the path.  This should not have any wildcards.
        
    Return:
        True if the path exists, False otherwise.
    """
    # ported from Dumbo
    shellcmd = "hadoop fs -stat '%s'"
    rval = _checked_hadoop_fs_command(shellcmd%(path))[0]
    return bool(int(rval == 0))
    
def rm(path):
    """Remove a file if it exists.
    
    Args:
        path: A string (potentially with wildcards).
    
    Return:
        True if the path was removed.  False otherwise.
    """
    shellcmd = "hadoop fs -rmr '%s'"%(path)
    rval, out, err = _checked_hadoop_fs_command(shellcmd)
    if rval is not 0:
        if rval is not 0:
            raise IOError('Ran[%s]: %s' % (path, err))
    return rval


def ls(path):
    """List files on HDFS.

    Args:
        path: A string (potentially with wildcards).

    Returns:
        A list of strings representing HDFS paths.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    rval, out, err = _checked_hadoop_fs_command('hadoop fs -ls %s' % path)
    
    if rval is not 0:
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
