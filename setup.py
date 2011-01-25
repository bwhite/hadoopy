#!/usr/bin/env python
from distutils.core import setup
from distutils.extension import Extension

# TODO: Only use Cython if it is available, else just use the .c
from Cython.Distutils import build_ext

ext_modules = [Extension("_main", ["hadoopy/_main.pyx"]),
               Extension("_typedbytes", ["hadoopy/_typedbytes.pyx"])]
setup(name='hadoopy',
      cmdclass={'build_ext': build_ext},
      version='.2',
      packages=['hadoopy'],
      author='Brandyn A. White',
      author_email='bwhite@dappervision.com',
      license='GPL',
      url='https://github.com/bwhite/hadoopy',
      ext_modules=ext_modules)
