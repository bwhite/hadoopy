#!/usr/bin/env python
import sys
import re
import subprocess
from distutils.core import setup
from distutils.extension import Extension
import glob
import os

# TODO: Only use Cython if it is available, else just use the .c
from Cython.Distutils import build_ext


def get_glibc_version():
    """
    Returns:
        Version as a pair of ints (major, minor) or None
    """
    # TODO: Look into a nicer way to get the version
    try:
        out = subprocess.Popen(['ldd', '--version'],
                               stdout=subprocess.PIPE).communicate()[0]
    except OSError:
        return
    match = re.search('([0-9]+)\.([0-9]+)\.?[0-9]*', out)
    try:
        return map(int, match.groups(1))
    except AttributeError:
        return


def _glob_recursive(glob_path):
    out = []
    for path in glob.glob(glob_path):
        if os.path.isdir(path):
            out += _glob_recursive(path + '/*')
        else:
            out.append(path)
    return out


def _remove_prefix(string, prefix='hadoopy/'):
    if string.startswith(prefix):
        return string[len(prefix):]


def _run_pyinstaller_configure():
    """Pyinstaller needs to run this once

    The resulting config is stored in hadoopy/thirdparty which will be copied
    along
    """
    subprocess.call(['python', 'hadoopy/thirdparty/pyinstaller/Configure.py'])

_run_pyinstaller_configure()
glibc_version = get_glibc_version()
tb_extra_args = []
if sys.byteorder != 'little':
    tb_extra_args.append('-D BYTECONVERSION_ISBIGENDIAN')

if glibc_version and (glibc_version[0] == 2 and glibc_version[1] >= 9):
    tb_extra_args.append('-D BYTECONVERSION_HASENDIAN_H')

# Since package_data doesn't handle directories, we find all of the files
thirdparty_paths = map(_remove_prefix, _glob_recursive('hadoopy/thirdparty/*'))
ext_modules = [Extension("_main", ["hadoopy/_main.pyx"]),
               Extension("_typedbytes", ["hadoopy/_typedbytes.pyx"],
                         extra_compile_args=tb_extra_args)]
setup(name='hadoopy',
      cmdclass={'build_ext': build_ext},
      version='0.3.0',
      packages=['hadoopy'],
      package_data={'hadoopy': thirdparty_paths},
      author='Brandyn A. White',
      author_email='bwhite@dappervision.com',
      license='GPL',
      url='https://github.com/bwhite/hadoopy',
      ext_modules=ext_modules)
