#!/usr/bin/env python
from distutils.core import setup
from distutils.extension import Extension
import glob
import os

# TODO: Only use Cython if it is available, else just use the .c
from Cython.Distutils import build_ext


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

# Since package_data doesn't handle directories, we find all of the files
thirdparty_paths = map(_remove_prefix, _glob_recursive('hadoopy/thirdparty/*'))
ext_modules = [Extension("_main", ["hadoopy/_main.pyx"]),
               Extension("_typedbytes", ["hadoopy/_typedbytes.pyx"])]
setup(name='hadoopy',
      cmdclass={'build_ext': build_ext},
      version='.2',
      packages=['hadoopy'],
      package_data={'hadoopy': thirdparty_paths},
      author='Brandyn A. White',
      author_email='bwhite@dappervision.com',
      license='GPL',
      url='https://github.com/bwhite/hadoopy',
      ext_modules=ext_modules)
