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
import gzip

from test_hdfs import hadoop_installed


def load_from_umiacs(path, md5hash):
    import hashlib
    import os
    import urllib
    name = os.path.basename(path)
    download = not os.path.exists(path)
    if os.path.exists(path) and md5hash:
        with open(path) as fp:
            if hashlib.md5(fp.read()).hexdigest() != md5hash:
                download = True
    if download:
        url = 'http://umiacs.umd.edu/~bwhite/%s' % name
        print('Downloading [%s]' % url)
        data = urllib.urlopen(url).read()
        with open(path, 'w') as fp:
            if md5hash:
                assert(md5hash == hashlib.md5(data).hexdigest())
            fp.write(data)


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
        load_from_umiacs('face_finder-input-voctrainpart.tb',
                         'dbc50c02103221a499fc7cc77a5b61e9')
        with open('haarcascade_frontalface_default.xml', 'w') as fp:
            o = gzip.GzipFile('haarcascade_frontalface_default.xml.gz').read()
            fp.write(o)
        self._run('face_finder-input-voctrainpart.tb')


if __name__ == '__main__':
    unittest.main()
