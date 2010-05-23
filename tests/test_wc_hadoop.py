#!/usr/bin/env python
import subprocess
import time
import unittest

import hadoopy


class TestWordcount(unittest.TestCase):

    def __init__(self, *args, **kw):
        super(TestWordcount, self).__init__(*args, **kw)
        self.data_path = 'hadoopy-test-data/%f/' % (time.time())
    
    def _run(self, fn):
        in_path = self.data_path + fn
        out_path = self.data_path + 'out-' + fn
        cmd = 'hadoop fs -put %s %s' % (fn,  in_path)
        subprocess.check_call(cmd.split())
        # Don't let the file split, CDH3 has a bug and will try to split gz's
        hadoopy.launch_frozen(in_path, out_path, 'wc.py', jobconfs='mapred.min.split.size=100000000')
        wc = dict(hadoopy.cat(out_path))
        self.assertEqual(wc['the'], 1664)
        self.assertEqual(wc['Alice'], 221)

    def test_textgz_in(self):
        self._run('wc-input-alice.txt.gz')

    def test_text_in(self):
        self._run('wc-input-alice.txt')

    def test_tb_in(self):
        self._run('wc-input-alice.tb')


if __name__ == '__main__':
    unittest.main()
