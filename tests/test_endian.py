#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import unittest
import subprocess


class Test(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)

    def test_fail(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall -D FAIL_TEST'.split())
        try:
            subprocess.check_call('./endian_test')
        except subprocess.CalledProcessError:
            pass

    def test_conversion(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall'.split())
        subprocess.check_call('./endian_test')

    def test_conversion_with_header(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall -D BYTECONVERSION_HASENDIAN_H'.split())
        subprocess.check_call('./endian_test')


if __name__ == '__main__':
    unittest.main()
