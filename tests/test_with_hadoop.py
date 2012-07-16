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
import hadoopy
import os
import numpy as np
import tempfile
import logging
import fetch_data
import pprint
logging.basicConfig(level=logging.DEBUG)

try:
    import unittest2 as unittest
except ImportError:
    import unittest


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
        fetch_data.main()
        self.data_path = 'hadoopy-test-data/%f/' % cur_time
        self.out_path = '%s/face_finder_out/%f/' % (tempfile.mkdtemp(), cur_time)
        os.makedirs(self.out_path)
        try:
            hadoopy.mkdir('hadoopy-test-data')
        except IOError:
            pass

    def setUp(self):
        try:
            hadoopy.mkdir(self.data_path)
        except IOError:
            pass

    def tearDown(self):
        if hadoopy.exists(self.data_path):
            self.assertTrue(hadoopy.isempty(self.data_path))  # directories are empty
            self.assertTrue(hadoopy.isdir(self.data_path))
            hadoopy.rmr(self.data_path)
        self.assertFalse(hadoopy.exists(self.data_path))
        self.assertFalse(hadoopy.isdir(self.data_path))
        self.assertFalse(hadoopy.isempty(self.data_path))

    # Face Finder test
    def _run_face(self, fn, **kw):
        in_path = self.data_path + fn
        out_path = '%sout-%s-%f' % (self.data_path, fn, time.time())
        if not hadoopy.exists(in_path):
            hadoopy.put(fn, in_path)
        hadoopy.launch_frozen(in_path, out_path, 'face_finder.py', files=['haarcascade_frontalface_default.xml'], **kw)
        for num, (image_name, (image_data, faces)) in enumerate(hadoopy.readtb(out_path)):
            with open(self.out_path + 'img%.8d.jpg' % num, 'w') as fp:
                fp.write(image_data)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    @unittest.skipIf(not pil_and_cv_installed(), 'PIL or OpenCV not installed')
    def test_face(self):
        self._run_face('../examples/data/face_finder-input-voctrainpart.tb')
        self._run_face('../examples/data/face_finder-input-voctrainpart.tb', pipe=False)

    def _run_wc(self, orig_fn, script_name='wc.py', launcher=hadoopy.launch_frozen, **kw):
        fn = 'out-%f-%s' % (time.time(), orig_fn)
        in_path = self.data_path + fn
        out_path = self.data_path + fn + '.out'
        print(os.path.abspath('.'))
        if not hadoopy.exists(in_path):
            hadoopy.put(orig_fn, in_path)
        # We also do a few hdfs checks here
        self.assertEquals(len(hadoopy.ls(in_path)), 1)
        self.assertEquals(hadoopy.ls(in_path), [hadoopy.abspath(in_path)])
        self.assertTrue(hadoopy.exists(in_path))
        self.assertFalse(hadoopy.exists(out_path))
        self.assertFalse(hadoopy.isdir(in_path))
        self.assertFalse(hadoopy.isempty(in_path))
        # Don't let the file split, CDH3 has a bug and will try to split gz's
        if not isinstance(launcher, str):
            launcher(in_path, out_path + '_list_jobconfs', script_name, jobconfs=['mapred.min.split.size=100000000',
                                                                                  'mapreduce.task.userlog.limit.kb=1000'], **kw)
            launcher(in_path, out_path, script_name, jobconfs={'mapred.min.split.size': '100000000',
                                                                    'mapreduce.task.userlog.limit.kb': '1000'}, **kw)
        if launcher == hadoopy.launch_frozen:
            self.assertTrue(hadoopy.isdir(out_path))
            self.assertTrue(hadoopy.isempty(out_path))  # Dirs are always empty
        elif launcher == hadoopy.launch_local:
            self.assertFalse(hadoopy.isdir(out_path))
            self.assertFalse(hadoopy.isempty(out_path))
        elif launcher == 'launch_frozen_cmd':
            cmd = 'python %s launch_frozen %s %s -jobconf "mapred.min.split.size=100000000" -jobconf "mapreduce.task.userlog.limit.kb=1000"' % (script_name,
                                                                                                                                                in_path,
                                                                                                                                                out_path)
            print(cmd)
            subprocess.call(cmd.split())
            self.assertTrue(hadoopy.isdir(out_path))
            self.assertTrue(hadoopy.isempty(out_path))  # Dirs are always empty
        else:
            raise ValueError('Launcher not recognized')
        wc = dict(hadoopy.readtb(out_path))
        self.assertEqual(wc['the'], 1664)
        self.assertEqual(wc['Alice'], 221)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_textgz_in(self):
        self._run_wc('wc-input-alice.txt.gz', launcher='launch_frozen_cmd')
        self._run_wc('wc-input-alice.txt.gz')
        self._run_wc('wc-input-alice.txt.gz', pipe=False)
        self._run_wc('wc-input-alice.txt.gz', script_name='wc_no_exec.py')
        self._run_wc('wc-input-alice.txt.gz', script_name='wc_no_exec.py', pipe=False)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_text_in(self):
        self._run_wc('wc-input-alice.txt', launcher='launch_frozen_cmd')
        self._run_wc('wc-input-alice.txt')
        self._run_wc('wc-input-alice.txt', pipe=False)
        self._run_wc('wc-input-alice.txt', script_name='wc_no_exec.py')
        self._run_wc('wc-input-alice.txt', script_name='wc_no_exec.py', pipe=False)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in(self):
        self._run_wc('wc-input-alice.tb', launcher='launch_frozen_cmd')
        self._run_wc('wc-input-alice.tb')
        self._run_wc('wc-input-alice.tb', pipe=False)
        self._run_wc('wc-input-alice.tb', script_name='wc_no_exec.py')
        self._run_wc('wc-input-alice.tb', script_name='wc_no_exec.py', pipe=False)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_textgz_in_local(self):
        self._run_wc('wc-input-alice.txt.gz', launcher=hadoopy.launch_local)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_text_in_local(self):
        self._run_wc('wc-input-alice.txt', launcher=hadoopy.launch_local)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_tb_in_local(self):
        self._run_wc('wc-input-alice.tb', launcher=hadoopy.launch_local)

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
        hadoopy.launch_local(out_path + '/wc-input-alice.tb', out_path + '/out_list_cmdenvs', 'local.py', max_input=1000,
                             cmdenvs=['TEST_ENV=10'],
                             files=['wc-input-alice.tb'])  # Just bring this along to test the files
        hadoopy.launch_local(out_path + '/wc-input-alice.tb', out_path + '/out', 'local.py', max_input=1000,
                             cmdenvs={'TEST_ENV': '10'},
                             files=['wc-input-alice.tb'])  # Just bring this along to test the files
        hadoopy.launch_local(((1000 * 'a', 10000000 * 'b') for x in range(100)), None, 'local.py', max_input=10000,
                             cmdenvs=['TEST_ENV=10'],
                             files=['wc-input-alice.tb'])

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_err(self):
        nonsense_path = 'sdfskjdfksjdkfjskdfksjdfksdkfjskdjfksjdk'
        self.assertFalse(hadoopy.exists(nonsense_path))
        self.assertEquals(hadoopy.abspath(nonsense_path).rsplit('/')[-1], nonsense_path)
        self.assertRaises(IOError, hadoopy.ls, nonsense_path)
        self.assertRaises(IOError, hadoopy.readtb(nonsense_path).next)

    @unittest.skipIf(not hadoop_installed(), 'Hadoop not installed')
    def test_cluster_info(self):
        hadoopy.writetb(self.out_path + 'cluster_info_input', [(0, 0)])
        hadoopy.launch_frozen(self.out_path + 'cluster_info_input', self.out_path + 'cluster_info', 'cluster_info.py')
        pprint.pprint(dict(hadoopy.readtb(self.out_path + 'cluster_info')))
        raise ValueError


if __name__ == '__main__':
    unittest.main()
