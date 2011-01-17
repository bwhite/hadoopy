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


def main():
    out = {}
    for fn in sorted(glob.glob('*.tb')):
        print(fn)
        with open(fn) as fp:
            data = fp.read()
        st = time.time()
        p = subprocess.Popen('python speed_hadoopy.py'.split(),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        o0 = p.communicate(data)[0]
        t0 = time.time() - st
        st = time.time()
        p = subprocess.Popen('python speed_hadoopyfp.py'.split(),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        o1 = p.communicate(data)[0]
        t1 = time.time() - st
        st = time.time()
        p = subprocess.Popen('python speed_tb.py'.split(),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        o2 = p.communicate(data)[0]
        t2 = time.time() - st
        p = subprocess.Popen('python speed_tbc.py'.split(),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        o3 = p.communicate(data)[0]
        t3 = time.time() - st
        print('%s:\tHadoopy[%2.6f] Hadoopyfp[%2.6f] Typedbytes[%2.6f] cTypedbytes[%2.6f]:  min(TypedBytes, cTypedBytes) / Hadoopyfp = %f' % (fn, t0, t1, t2, t3, min(t2, t3) / t1))
        assert(o0 == o1 == o2 == o3)
        #for x, y, z in itertools.izip(parse_tb(o0), parse_tb(o1), parse_tb(o2)):
        #    try:
        #        assert(x == y == z)
        #    except AssertionError, e:
        #        print('x:%r\ny:%r\nz:%r' % (x, y, z))
        #        raise e
        out[fn] = (t0, t1, t2)


if __name__ == '__main__':
    main()
