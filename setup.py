#!/usr/bin/env python
import sys
import re
import subprocess
from distutils.core import setup
from distutils.extension import Extension

# TODO: Only use Cython if it is available, else just use the .c
from Cython.Distutils import build_ext

import platform
glibc_version = map(int, platform.libc_ver()[1].split('.'))

tb_extra_args = []
if sys.byteorder != 'little':
    tb_extra_args.append('-D BYTECONVERSION_ISBIGENDIAN')

if glibc_version[0] > 2 or (glibc_version[0] == 2 and glibc_version[1] >= 9):
    tb_extra_args.append('-D BYTECONVERSION_HASENDIAN_H')

ext_modules = [Extension("_main", ["hadoopy/_main.pyx"]),
               Extension("_typedbytes", ["hadoopy/_typedbytes.pyx"],
                         extra_compile_args=tb_extra_args)]
setup(name='hadoopy',
      cmdclass={'build_ext': build_ext},
      version='.2',
      packages=['hadoopy'],
      author='Brandyn A. White',
      author_email='bwhite@dappervision.com',
      license='GPL',
      url='https://github.com/bwhite/hadoopy',
      ext_modules=ext_modules)
