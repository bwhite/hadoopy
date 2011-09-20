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
import re
import os
import hadoopy
from hadoopy._runner import _find_hstreaming


def _cleaned_hadoop_stderr(hdfs_stderr):
    for line in hdfs_stderr:
        parts = line.split()
        if parts[2] == 'INFO' or parts[2] == 'WARN':
            pass
        else:
            yield line
        

def _hadoop_fs_command(cmd, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, java_mem_mb=100):
    env = dict(os.environ)
    env['HADOOP_OPTS'] = "-Xmx%dm" % java_mem_mb
    p = subprocess.Popen(cmd, env=env, shell=True, close_fds=True,
                         stdin=stdin,
                         stdout=stdout,
                         stderr=stderr)
    return p


def _checked_hadoop_fs_command(cmd, *args, **kw):
    p = _hadoop_fs_command(cmd, *args, **kw)
    stdout, stderr = p.communicate()
    rcode = p.returncode
    if rcode is not 0:
        raise IOError('Ran[%s]: %s' % (cmd, stderr))
    return rcode, stdout, stderr


def exists(path):
    """Check if a file exists.

    :param path: A string for the path.  This should not have any wildcards.
    :returns: True if the path exists, False otherwise.
    """
    cmd = "hadoop fs -test -e %s"
    p = _hadoop_fs_command(cmd % (path))
    p.communicate()
    rcode = p.returncode
    return bool(int(rcode == 0))


def isdir(path):
    """Check if a path is a directory

    :param path: A string for the path.  This should not have any wildcards.
    :returns: True if the path is a directory, False otherwise.
    """
    cmd = "hadoop fs -test -d %s"
    p = _hadoop_fs_command(cmd % (path))
    p.communicate()
    rcode = p.returncode
    return bool(int(rcode == 0))


def isempty(path):
    """Check if a path has zero length (also true if it's a directory)

    :param path: A string for the path.  This should not have any wildcards.
    :returns: True if the path has zero length, False otherwise.
    """
    cmd = "hadoop fs -test -z %s"
    p = _hadoop_fs_command(cmd % (path))
    p.communicate()
    rcode = p.returncode
    return bool(int(rcode == 0))


_USER_HOME_DIR = None  # Cache for user's home directory


def abspath(path):
    """Return the absolute path to a file and canonicalize it

    Path is returned without a trailing slash and without redundant slashes.
    Caches the user's home directory.

    :param path: A string for the path.  This should not have any wildcards.
    :returns Absolute path to the file
    :raises IOError: If unsuccessful
    """
    global _USER_HOME_DIR
    # FIXME(brandyn): User's home directory must exist
    if path[0] == '/':
        return os.path.abspath(path)
    if _USER_HOME_DIR is None:
        try:
            _USER_HOME_DIR = hadoopy.ls('.')[0].rsplit('/', 1)[0]
        except IOError, e:
            if not exists('.'):
                raise IOError("Home directory doesn't exist")
            raise e
    return os.path.abspath(os.path.join(_USER_HOME_DIR, path))


def rmr(path):
    """Remove a file if it exists (recursive)

    :param path: A string (potentially with wildcards).
    :raises IOError: If unsuccessful
    """
    cmd = "hadoop fs -rmr %s" % (path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def mv(hdfs_src, hdfs_dst):
    """Put a file on hdfs
    
    :param hdfs_src: Source (str)
    :param hdfs_dst: Destination (str)
    :raises: IOError: If unsuccessful
    """
    cmd = "hadoop fs -mv %s %s" % (hdfs_src, hdfs_dst)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def put(local_path, hdfs_path):
    """Put a file on hdfs
    
    :param local_path: Source (str)
    :param hdfs_path: Destination (str)
    :raises: IOError: If unsuccessful
    """
    cmd = "hadoop fs -put %s %s" % (local_path, hdfs_path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def get(hdfs_path, local_path):
    """Get a file from hdfs

    :param hdfs_path: Destination (str)
    :param local_path: Source (str)
    :raises: IOError: If unsuccessful
    """
    cmd = "hadoop fs -get %s %s" % (hdfs_path, local_path)
    rcode, stdout, stderr = _checked_hadoop_fs_command(cmd)


def ls(path):
    """List files on HDFS.

    :param path: A string (potentially with wildcards).
    :rtype: A list of strings representing HDFS paths.
    :raises: IOError: An error occurred listing the directory (e.g., not available).
    """
    rcode, stdout, stderr = _checked_hadoop_fs_command('hadoop fs -ls %s' % path)
    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    out = [x.split(' ')[-1] for x in stdout.split('\n')
           if x and not found_line(x)]
    return out


def writetb(path, kvs, java_mem_mb=256):
    """Write typedbytes sequence file to HDFS given an iterator of KeyValue pairs

    :param path: HDFS path (string)
    :param kvs: Iterator of (key, value)
    :param java_mem_mb: Integer of java heap size in MB (default 256)
    :raises: IOError: An error occurred while saving the data.
    """
    read_fd, write_fd = os.pipe()
    read_fp = os.fdopen(read_fd, 'r')
    hstreaming = _find_hstreaming()
    cmd = 'hadoop jar %s loadtb %s' % (hstreaming, path)
    p = _hadoop_fs_command(cmd, stdin=read_fp, java_mem_mb=java_mem_mb)
    read_fp.close()
    with hadoopy.TypedBytesFile(write_fd=write_fd) as tb_fp:
        for kv in kvs:
            if p.poll() is not None:
                raise IOError('Child process quit while we were sending it data. STDOUT[%s] STDERR[%s]' % p.communicate())
            tb_fp.write(kv)
        tb_fp.flush()
    p.wait()
    if p.returncode is not 0:
        raise IOError('writetb: Child returned [%d] Stderr[%s]' % (p.returncode, p.stderr.read()))


def readtb(paths, num_procs=10, java_mem_mb=256, ignore_logs=True):
    """Read typedbytes sequence files on HDFS (with optional compression).

    By default, ignores files who's names start with an underscore '_' as they
    are log files.  This allows you to cat a directory that may be a variety of
    outputs from hadoop (e.g., _SUCCESS, _logs).  This works on directories and
    files.  The KV pairs may be interleaved between files
    (they are read in parallel).

    :param paths: HDFS path (str) or paths (iterator)
    :param num_procs: Number of reading procs to open (default 10)
    :param java_mem_mb: Integer of java heap size in MB (default 256)
    :param ignore_logs: If True, ignore all files who's name starts with an underscore.  Defaults to True.
    :returns: An iterator of key, value pairs.
    :raises: IOError: An error occurred reading the directory (e.g., not available).
    """
    import select
    hstreaming = _find_hstreaming()
    if isinstance(paths, str):
        paths = [paths]
    read_fds = set()
    procs = {}
    tb_fps = {}

    def _open_tb(cur_path):
        cmd = 'hadoop jar %s dumptb %s' % (hstreaming, cur_path)
        read_fd, write_fd = os.pipe()
        write_fp = os.fdopen(write_fd, 'w')
        p = _hadoop_fs_command(cmd, stdout=write_fp, java_mem_mb=java_mem_mb)
        write_fp.close()
        read_fds.add(read_fd)
        procs[read_fd] = p
        tb_fps[read_fd] = hadoopy.TypedBytesFile(read_fd=read_fd)

    def _path_gen():
        for root_path in paths:
            try:
                all_paths = ls(root_path)
            except IOError:
                raise IOError("No such file or directory: '%s'" % root_path)
            if ignore_logs:
                # Ignore any files that start with an underscore
                keep_file = lambda x: os.path.basename(x)[0] != '_'
                all_paths = filter(keep_file, all_paths)
            for cur_path in all_paths:
                yield _open_tb(cur_path)
    try:
        path_gen = _path_gen()
        for x in range(num_procs):
            try:
                path_gen.next()
            except (AttributeError, StopIteration):
                path_gen = None
        while read_fds:
            cur_fds = select.select(read_fds, [], [])[0]
            for read_fd in cur_fds:
                p = procs[read_fd]
                tp_fp = tb_fps[read_fd]
                try:
                    yield tp_fp.next()
                except StopIteration:
                    p.wait()
                    del procs[read_fd]
                    del tb_fps[read_fd]
                    del p
                    os.close(read_fd)
                    read_fds.remove(read_fd)
                    try:
                        path_gen.next()
                    except (AttributeError, StopIteration):
                        path_gen = None
    finally:
        # Cleanup outstanding procs
        for p in procs.values():
            p.kill()
            p.wait()
