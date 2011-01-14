#!/usr/bin/env python
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

ext_modules = [Extension("core", ["hadoopy/hadoopy.pyx"]),
               Extension("typedbytes", ["hadoopy/typedbytes.pyx"])]
setup(name='hadoopy',
      cmdclass={'build_ext': build_ext},
      version='.2',
      packages=['hadoopy'],
      ext_modules=ext_modules)
