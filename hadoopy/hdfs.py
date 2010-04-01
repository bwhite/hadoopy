import subprocess
import typedbytes
import tempfile

from hadoopy.runner import _find_hstreaming


def hdfs_cat_tb(path):
    """Used to read files off of HDFS that have been stored as
    typedbytes in a sequence file and optionally compressed.
    Wildcards are acceptable. The output format is an iterator of
    key, value pairs."""
    fp = tempfile.TemporaryFile()
    subprocess.Popen(['hadoop', 'jar', _find_hstreaming(), 'dumptb', path],
                     stdout=fp).wait()
    fp.seek(0)
    return typedbytes.PairedInput(fp).reads()
