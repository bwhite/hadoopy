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

__author__ =  'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import unittest
import hadoopy
import tempfile
import subprocess
import os
import re


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
        return map(int, match.groups())
    except AttributeError:
        return


def has_endian():
    glibc_version = get_glibc_version()
    if glibc_version and (glibc_version[0] == 2 and glibc_version[1] >= 9):
        return True


class Test(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(Test, self).__init__(*args, **kw)

    def test_int(self):
        f = tempfile.NamedTemporaryFile()
        kvs = [(0, 0), (-123, 123), (-1, 1), (-2**32, 2**32), (-2**64, 2**64),
               (-2**128, 2**128), (-2**256, 2**256)]
        for kv in kvs:
            with hadoopy.TypedBytesFile(f.name, 'w') as fp:
                fp.write(kv)
            self.assertEquals(hadoopy.TypedBytesFile(f.name, 'r').next(), kv)

    @unittest.skipIf(not has_endian(), 'Needs endian.h which is in a newer glibc')
    def test_fail(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall -D FAIL_TEST'.split())
        try:
            subprocess.check_call('./endian_test')
        except subprocess.CalledProcessError:
            pass

    @unittest.skipIf(not has_endian(), 'Needs endian.h which is in a newer glibc')
    def test_conversion(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall'.split())
        subprocess.check_call('./endian_test')

    @unittest.skipIf(not has_endian(), 'Needs endian.h which is in a newer glibc')
    def test_conversion_with_header(self):
        subprocess.check_call('gcc -o endian_test endian_test.c -Wall -D BYTECONVERSION_HASENDIAN_H'.split())
        subprocess.check_call('./endian_test')

    def test_status(self):
        def err(x):
            self.assertEqual('reporter:status:[test]\n', x)
        hadoopy.status('[test]', err=err)

    def test_counter(self):
        def err(x):
            self.assertEqual('reporter:counter:a,b,5\n', x)
        hadoopy.counter('a', 'b', 5, err=err)

    def test_int_pipe(self):
        read_write_fds = os.pipe()
        kvs = [(0, 0), (-123, 123), (-1, 1), (-2**32, 2**32), (-2**64, 2**64),
               (-2**128, 2**128), (-2**256, 2**256)]
        with hadoopy.TypedBytesFile(read_fd=read_write_fds[0]) as fp1:
            with hadoopy.TypedBytesFile(write_fd=read_write_fds[1]) as fp0:
                for num, kv in enumerate(kvs):
                    print(kv)
                    print('Setting')
                    fp0.write(kv)
                    fp0.flush()
                    self.assertEquals(fp1.next(), kv)
                # NOTE: If we read again here then it will block forever
            # Try to read one even when there are none left
            self.assertRaises(StopIteration, fp1.next)


class HadoopyTest(hadoopy.Test):
    def test_wc(self):
        from wc_class import Mapper, Reducer
        test_in = [(None, 'a'),
                   (None, 'b'),
                   (None, 'a'),
                   (None, 'c'),
                   (None, 'a')]
        test_map_out = [('a', 3),
                        ('c', 1),
                        ('b', 1)]
        test_reduce_out = [('a', 3),
                           ('b', 1),
                           ('c', 1)]
        self.assertEqual(self.call_map(Mapper, test_in), test_map_out)
        reduce_in = self.groupby_kv(self.sort_kv(test_map_out))
        self.assertEqual(self.call_reduce(Reducer, reduce_in), test_reduce_out)

    def test_wc_tb(self):
        # The mapper takes resplit words
        from wc_tb import mapper, reducer
        test_in = [(None, 'a'),
                   (None, 'b'),
                   (None, 'a'),
                   (None, 'c'),
                   (None, 'a')]
        test_map_out = [('a', 1),
                        ('b', 1),
                        ('a', 1),
                        ('c', 1),
                        ('a', 1)]
        test_reduce_out = [('a', 3),
                           ('b', 1),
                           ('c', 1)]
        self.assertEqual(self.call_map(mapper, test_in), test_map_out)
        reduce_in = self.groupby_kv(self.sort_kv(test_map_out))
        self.assertEqual(self.call_reduce(reducer, reduce_in), test_reduce_out)



if __name__ == '__main__':
    unittest.main()
