import subprocess

from hadoopy.runner import _find_hstreaming


def cat_tb(path):
    """Used to read relatively small files off of HDFS that have been stored as
    typedbytes in a sequence file and optionally compressed. Note that this
    will load all of the file(s) into memory.  Wildcards are acceptable.
    The output format is an iterator of key, value pairs."""
    p = subprocess.Popen(['hadoop', 'jar', _fine_hstreaming(), path],
                         stdout=subprocess.PIPE)
    return typedbytes.PairedInput(p.communicate()[0]).reads()
