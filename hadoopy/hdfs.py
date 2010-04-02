import subprocess
import typedbytes
import tempfile
import re

from hadoopy.runner import _find_hstreaming


def hdfs_ls(path):
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

def hdfs_cat_tb(path):
    """Read typedbytes sequence files on HDFS (with optional compression).

    Args:
      path: A string (potentially with wildcards).

    Returns:
      An iterator of key, value pairs.

    Raises:
      IOError: An error occurred listing the directory (e.g., not available).
    """
    fp = tempfile.TemporaryFile()
    for x in hdfs_ls(path):
        subprocess.Popen(['hadoop', 'jar', _find_hstreaming(), 'dumptb', x],
                         stdout=fp, stderr=subprocess.PIPE).wait()
    fp.seek(0)
    return typedbytes.PairedInput(fp).reads()
