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

import subprocess
import time
import unittest
import hadoopy
import os
import gzip
import numpy as np
import hashlib
import urllib


def load_from_umiacs(path, md5hash):
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


def hadoop_installed():
    try:
        subprocess.Popen('hadoop', stderr=subprocess.PIPE,
                         stdout=subprocess.PIPE)
    except OSError:
        return False
    return True


def pil_and_cv_installed():
    try:
        import Image
        import cv
    except ImportError:
        return False
    return True


def readtb(paths, ignore_logs=True):
    """Read typedbytes sequence files on HDFS (with optional compression).

    By default, ignores files who's names start with an underscore '_' as they
    are log files.  This allows you to cat a directory that may be a variety of
    outputs from hadoop (e.g., _SUCCESS, _logs).  This works on directories and
    files.

    Args:
        paths: HDFS path (str) or paths (iterator)
        ignore_logs: If True, ignore all files who's name starts with an
            underscore.  Defaults to True.

    Returns:
        An iterator of key, value pairs.

    Raises:
        IOError: An error occurred listing the directory (e.g., not available).
    """
    hstreaming = hadoopy._runner._find_hstreaming()
    if isinstance(paths, str):
        paths = [paths]
    for root_path in paths:
        all_paths = hadoopy.ls(root_path)
        if ignore_logs:
            # Ignore any files that start with an underscore
            keep_file = lambda x: os.path.basename(x)[0] != '_'
            all_paths = filter(keep_file, all_paths)
        for cur_path in all_paths:
            cmd = 'hadoop jar %s dumptb %s' % (hstreaming, cur_path)
            read_fd, write_fd = os.pipe()
            write_fp = os.fdopen(write_fd, 'w')
            p = hadoopy._hdfs._hadoop_fs_command(cmd, stdout=write_fp)
            write_fp.close()
            with hadoopy.TypedBytesFile(read_fd=read_fd) as tb_fp:
                for kv in tb_fp:
                    yield kv
            p.wait()


class TestUsingHadoop(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(TestUsingHadoop, self).__init__(*args, **kw)
        cur_time = time.time()
        self.data_path = 'hadoopy-test-data/%f/' % cur_time
        self.out_path = 'face_finder_out/%f/' % cur_time
        os.makedirs(self.out_path)
        load_from_umiacs('face_finder-input-voctrainpart.tb',
                         'dbc50c02103221a499fc7cc77a5b61e9')

    def tearDown(self):
        if hadoopy.exists(self.data_path):
            self.assertTrue(hadoopy.isempty(self.data_path))  # directories are empty
            self.assertTrue(hadoopy.isdir(self.data_path))
            hadoopy.rmr(self.data_path)
        self.assertFalse(hadoopy.exists(self.data_path))
        self.assertFalse(hadoopy.isdir(self.data_path))
        self.assertFalse(hadoopy.isempty(self.data_path))

    # Face Finder test
    def _run_face(self, fn):
        in_path = self.data_path + fn
        out_path = self.data_path + 'out-' + fn
        cmd = 'hadoop fs -put %s %s' % (fn, in_path)
        subprocess.check_call(cmd.split())
        hadoopy.launch_frozen(in_path, out_path, 'face_finder.py', reducer=False, files=['haarcascade_frontalface_default.xml'])
        for num, (image_name, (image_data, faces)) in enumerate(hadoopy.readtb(out_path)):
            with open(self.out_path + 'img%.8d.jpg' % num, 'w') as fp:
                fp.write(image_data)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    @unittest.skipIf(not pil_and_cv_installed(), 'PIL or OpenCV not installed')
    def test_face(self):
        with open('haarcascade_frontalface_default.xml', 'w') as fp:
            o = gzip.GzipFile('haarcascade_frontalface_default.xml.gz').read()
            fp.write(o)
        self._run_face('face_finder-input-voctrainpart.tb')

    def _run_wc(self, orig_fn, launcher=hadoopy.launch_frozen):
        fn = 'out-%f-%s' % (time.time(), orig_fn)
        in_path = self.data_path + fn
        out_path = self.data_path + fn + '.out'
        hadoopy.put(orig_fn, in_path)
        # We also do a few hdfs checks here
        self.assertEquals(len(hadoopy.ls(in_path)), 1)
        self.assertEquals(hadoopy.ls(in_path), [hadoopy.abspath(in_path)])
        self.assertTrue(hadoopy.exists(in_path))
        self.assertFalse(hadoopy.exists(out_path))
        self.assertFalse(hadoopy.isdir(in_path))
        self.assertFalse(hadoopy.isempty(in_path))
        # Don't let the file split, CDH3 has a bug and will try to split gz's
        launcher(in_path, out_path, 'wc.py', jobconfs=['mapred.min.split.size=100000000',
                                                       'mapreduce.task.userlog.limit.kb=1000'])
        if launcher == hadoopy.launch_frozen:
            self.assertTrue(hadoopy.isdir(out_path))
            self.assertTrue(hadoopy.isempty(out_path))  # Dirs are always empty
        elif launcher == hadoopy.launch_local:
            self.assertFalse(hadoopy.isdir(out_path))
            self.assertFalse(hadoopy.isempty(out_path))
        else:
            raise ValueError('Launcher not recognized')
        wc = dict(hadoopy.readtb(out_path))
        self.assertEqual(wc['the'], 1664)
        self.assertEqual(wc['Alice'], 221)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_textgz_in(self):
        self._run_wc('wc-input-alice.txt.gz')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_text_in(self):
        self._run_wc('wc-input-alice.txt')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in(self):
        self._run_wc('wc-input-alice.tb')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_textgz_in_local(self):
        self._run_wc('wc-input-alice.txt.gz', hadoopy.launch_local)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_text_in_local(self):
        self._run_wc('wc-input-alice.txt', hadoopy.launch_local)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in_local(self):
        self._run_wc('wc-input-alice.tb', hadoopy.launch_local)

    def _run_hdfs(self, orig_fn):
        fn = '%f-%s' % (time.time(), orig_fn)
        file_path = '%s/%s' % (self.data_path, fn)
        hadoopy.put(orig_fn, file_path)
        cat_output = [_ for _ in hadoopy.readtb(file_path)]
        line = (331, 'Title: Alice\'s Adventures in Wonderland')
        self.assertTrue(line in cat_output)
        ls_output = hadoopy.ls(self.data_path)
        self.assertTrue([x for x in ls_output if x.rsplit('/', 1)[-1] == fn])
        ls_output = hadoopy.ls(file_path)
        self.assertTrue(ls_output[0].rsplit('/', 1)[-1] == fn)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_hdfs(self):
        self._run_hdfs('wc-input-alice.txt')
        self._run_hdfs('wc-input-alice.tb')
        self._run_hdfs('wc-input-alice.txt.gz')

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_err(self):
        nonsense_path = 'sdfskjdfksjdkfjskdfksjdfksdkfjskdjfksjdk'
        self.assertFalse(hadoopy.exists(nonsense_path))
        self.assertEquals(hadoopy.abspath(nonsense_path).rsplit('/')[-1], nonsense_path)
        self.assertRaises(IOError, hadoopy.ls, nonsense_path)
        self.assertRaises(IOError, hadoopy.readtb(nonsense_path).next)

    def _readtb(self, reader, path):
        out = []
        st = time.time()
        for x, y in reader(path):
            out.append(x)
        out.sort()
        print('Reader Time[%s][%f]' % (repr(reader), time.time() - st))
        return sorted(out)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_readtb_writetb(self):
        working_path = '%s/readtb_writetb/' % (self.data_path)
        self.assertFalse(hadoopy.exists(working_path))
        self.assertFalse(hadoopy.isdir(working_path))
        self.assertFalse(hadoopy.isempty(working_path))
        for x in range(10):
            fn = '%s/%.5d' % (working_path, x)
            print(fn)
            data = [('1', 1), (1.3, np.array([1, 2, 3])), (True, {'1': 3})]
            hadoopy.writetb(fn, data)
        self.assertFalse(hadoopy.isdir(fn))
        self.assertFalse(hadoopy.isempty(fn))
        self.assertTrue(hadoopy.isdir(working_path))
        self.assertTrue(hadoopy.isempty(working_path))  # isempty returns true on directories
        self.assertEqual(self._readtb(readtb, working_path),
                         self._readtb(hadoopy.readtb, working_path))

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_find(self):
        self.assertTrue(hadoopy._runner._find_hstreaming())

    def test_local(self):
        out_path = '%s/local_test/%f' % (self.data_path, time.time())
        hadoopy.put('wc-input-alice.tb', out_path + '/wc-input-alice.tb')
        hadoopy.launch_local(out_path + '/wc-input-alice.tb', out_path + '/out', 'local.py', max_input=1000,
                             cmdenvs=['TEST_ENV=10'],
                             files=['mytest_dir/test_file'])
        hadoopy.launch_local(((1000 * 'a', 10000000 * 'b') for x in range(100)), None, 'local.py', max_input=10000,
                             cmdenvs=['TEST_ENV=10'],
                             files=['mytest_dir/test_file'])


if __name__ == '__main__':
    unittest.main()
