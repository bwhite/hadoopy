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

import subprocess
import time
import unittest
import hadoopy
import os

from test_hdfs import hadoop_installed


class TestFaceFinderHadoop(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(TestFaceFinderHadoop, self).__init__(*args, **kw)
        cur_time = time.time()
        self.data_path = 'hadoopy-test-data/%f/' % cur_time
        self.out_path = 'face_finder_out/%f/' % cur_time
        os.makedirs(self.out_path)
    
    def _run(self, fn):
        in_path = self.data_path + fn
        out_path = self.data_path + 'out-' + fn
        cmd = 'hadoop fs -put %s %s' % (fn,  in_path)
        subprocess.check_call(cmd.split())
        hadoopy.launch_frozen(in_path, out_path, 'face_finder.py', reducer=False, files=['haarcascade_frontalface_default.xml'])
        for num, (image_name, (image_data, faces)) in enumerate(hadoopy.cat(out_path)):
            with open(self.out_path + 'img%.8d.jpg' % num, 'w') as fp:
                fp.write(image_data)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in(self):
        self._run('face_finder-input-voctrainpart.tb')


if __name__ == '__main__':
    unittest.main()
