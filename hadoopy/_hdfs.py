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
import os
import multiprocessing
import hadoopy
from hadoopy._runner import _find_hstreaming


def _cleaned_hadoop_stderr(hdfs_stderr):
    for line in hdfs_stderr:
        parts = line.split()
        if parts[2] == 'INFO' or parts[2] == 'WARN':
            pass
        else:
            yield line
        

def _checked_hadoop_fs_command(cmd):

    p = subprocess.Popen(cmd.split(), env={}, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode:
        p = subprocess.Popen(cmd.split(),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
    print(cmd.split())
    rcode = p.returncode
    return rcode, out, err


def exists(path):
    """Check if a file exists.
    
    Args:
        path: A string for the path.  This should not have any wildcards.
        
    Return:
        True if the path exists, False otherwise.
    """
    shellcmd = "hadoop fs -stat %s"
    rval = _checked_hadoop_fs_command(shellcmd % (path))[0]
    return bool(int(rval == 0))
    

def rm(path):
    """Remove a file if it exists.
    
    Args:
        path: A string (potentially with wildcards).

    Raises:
        IOError: If unsuccessful
    """
    shellcmd = "hadoop fs -rmr %s" % (path)
    rval, out, err = _checked_hadoop_fs_command(shellcmd)
    if rval is not 0:
        raise IOError('Ran[%s]: %s' % (shellcmd, err))


def put(local_path, hdfs_path):
    """Put a file on hdfs
    
    Args:
        local_path: Source (str)
        hdfs_path: Destrination (str)

    Raises:
        IOError: If unsuccessful
    """
    shellcmd = "hadoop fs -put %s %s" % (local_path, hdfs_path)
    rval, out, err = _checked_hadoop_fs_command(shellcmd)
    if rval is not 0:
        raise IOError('Ran[%s]: %s' % (shellcmd, err))


def get(hdfs_path, local_path):
    """Get a file from hdfs
    
    Args:
        hdfs_path: Destrination (str)
        local_path: Source (str)

    Raises:
        IOError: If unsuccessful
    """
    shellcmd = "hadoop fs -get %s %s" % (hdfs_path, local_path)
    rval, out, err = _checked_hadoop_fs_command(shellcmd)
    if rval is not 0:
        raise IOError('Ran[%s]: %s' % (shellcmd, err))


def ls(path):
    """List files on HDFS.

    Args:
        path: A string (potentially with wildcards).

    Returns:
        A list of strings representing HDFS paths.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    try:
        # This one works while inside of a running job
        # One of the environmental variables set in a job breaks
        # normal execution, resulting in permission denied on the .pid
        p = subprocess.Popen('hadoop fs -ls %s' % path, env={},
                             shell=True, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode:
            raise IOError
    except IOError:
        # This one works otherwise
        p = subprocess.Popen('hadoop fs -ls %s' % path,
                              shell=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode:
            raise IOError('Ran[%s]: %s' % (path, err))

    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    out = [x.split(' ')[-1] for x in out.split('\n')
           if x and not found_line(x)]
    return out


def writetb(path, kvs):
    """Write typedbytes sequence file on local disk
    """
    read_fd, write_fd = os.pipe()
    read_fp = os.fdopen(read_fd, 'r')
    hstreaming = _find_hstreaming()
    cmd = ('hadoop jar %s loadtb %s' % (hstreaming, path)).split()
    # TODO(brandyn): Make this work inside of a hadoop task like it did
    p = subprocess.Popen(cmd,
                         stdin=read_fp, close_fds=True,
                         stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    with hadoopy.TypedBytesFile(write_fd=write_fd) as tb_fp:
        for kv in kvs:
            tb_fp.write(kv)
        tb_fp.flush()
    p.wait()


def readtb(path, ignore_logs=True):
    """Read typedbytes sequence files on HDFS (with optional compression).

    By default, ignores files who's names start with an underscore '_' as they
    are log files.  This allows you to cat a directory that may be a variety of
    outputs from hadoop (e.g., _SUCCESS, _logs).

    Args:
        path: A string (potentially with wildcards).
        ignore_logs: If True, ignore all files who's name starts with an
            underscore.  Defaults to True.

    Returns:
        An iterator of key, value pairs.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    hstreaming = _find_hstreaming()
    all_paths = ls(path)
    if ignore_logs:
        # Ignore any files that start with an underscore
        keep_file = lambda x: os.path.basename(x)[0] != '_'
        all_paths = filter(keep_file, all_paths)
    for cur_path in all_paths:
        cmd = ('hadoop jar %s dumptb %s' % (hstreaming, cur_path)).split()
        read_fd, write_fd = os.pipe()
        write_fp = os.fdopen(write_fd, 'w')
        # TODO(brandyn): Make this work inside of a hadoop task like it did
        p = subprocess.Popen(cmd, stdout=write_fp,
                             stderr=subprocess.PIPE)
        write_fp.close()
        with hadoopy.TypedBytesFile(read_fd=read_fd) as tb_fp:
            for kv in tb_fp:
                yield kv
        p.wait()
