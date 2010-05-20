import subprocess
import typedbytes
import tempfile
import re
import multiprocessing

from hadoopy.runner import _find_hstreaming


def ls(path):
    """List files on HDFS.

    Args:
        path: A string (potentially with wildcards).

    Returns:
        A list of strings representing HDFS paths.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    out, err = subprocess.Popen(['hadoop', 'fs', '-ls', path],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    if err:
        raise IOError, err
    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    out = [x.split(' ')[-1] for x in out.split('\n') if x and not found_line(x)]
    return out


def _hdfs_cat_tb(args):
    path, hstreaming, fn = args
    with open(fn, 'wb') as fp:
        subprocess.Popen(['hadoop', 'jar', hstreaming, 'dumptb', path],
                         stdout=fp, stderr=subprocess.PIPE).wait()


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
        p.map(_hdfs_cat_tb, [(path, hstreaming, fp.name) for path, fp in zip(paths, fps)])
        for y in fps:
            for z in typedbytes.PairedInput(y).reads():
                yield z
