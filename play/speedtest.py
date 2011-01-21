import glob
import subprocess
import time
import typedbytes
import cStringIO as StringIO
import itertools


def parse_tb(val):
    fp = StringIO.StringIO(val)
    for x in typedbytes.PairedInput(fp):
        yield x


def time_script(script_name, data):
    st = time.time()
    p = subprocess.Popen(('python %s' % script_name).split(),
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    o = p.communicate(data)[0]
    return o, time.time() - st


def main():
    out = []
    print('+-----------------+---------+---------+---------+---------+---------+')
    print('|Filename         |Hadoopy  |HadoopyFP|TB       |cTB      |Ratio    |')
    print('+=================+=========+=========+=========+=========+=========+')
    for fn in sorted(glob.glob('*.tb')):
        with open(fn) as fp:
            data = fp.read()
        o0, t0 = time_script('speed_hadoopy.py', data)
        o1, t1 = time_script('speed_hadoopyfp.py', data)
        o2, t2 = time_script('speed_tb.py', data)
        o3, t3 = time_script('speed_tbc.py', data)
        out.append((fn, t0, t1, t2, t3, min([t2, t3]) / t1))
        assert(o0 == o1 == o2 == o3)
    out.sort(lambda x, y: cmp(x[-1], y[-1]), reverse=True)
    for x in out:
        print('|%17s|%9.6f|%9.6f|%9.6f|%9.6f|%9.6f|' % x)
        print('+-----------------+---------+---------+---------+---------+---------+')
if __name__ == '__main__':
    main()
