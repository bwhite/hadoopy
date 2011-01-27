#!/usr/bin/env python
import sys
import os
from distutils.core import setup
from distutils.extension import Extension

# TODO: Only use Cython if it is available, else just use the .c
from Cython.Distutils import build_ext

tb_extra_args = []
if sys.byteorder != 'little':
    tb_extra_args.append('-D BYTECONVERSION_ISBIGENDIAN')

# TODO: Make a better check for this
if os.path.isfile('/usr/include/endian.h'):
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
