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

import time
import unittest
import hadoopy

from test_hdfs import hadoop_installed


class TestWordcountHadoop(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(TestWordcountHadoop, self).__init__(*args, **kw)
        self.data_path = 'hadoopy-test-data/%f/' % (time.time())
    
    def _run(self, fn):
        in_path = self.data_path + fn
        out_path = self.data_path + 'out-' + fn
        hadoopy.put(fn, in_path)
        # Don't let the file split, CDH3 has a bug and will try to split gz's
        hadoopy.launch_local(in_path, out_path, 'wc.py', jobconfs='mapred.min.split.size=100000000', combiner=True)
        wc = dict(hadoopy.readtb(out_path))
        self.assertEqual(wc['the'], 1664)
        self.assertEqual(wc['Alice'], 221)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_textgz_in(self):
        self._run('wc-input-alice.txt.gz')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_text_in(self):
        self._run('wc-input-alice.txt')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in(self):
        self._run('wc-input-alice.tb')


if __name__ == '__main__':
    unittest.main()
