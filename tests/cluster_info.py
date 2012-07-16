import hadoopy
import subprocess
import os


class Mapper(object):

    def __init__(self):
        self.data_output = False

    def map(self, k, v):
        if self.data_output:  # Only run on first KV pair
            return
        self.data_output = True
        # libstdc++ version
        stdversions = subprocess.Popen('strings /usr/lib/libstdc++.so.6'.split(), stdout=subprocess.PIPE).communicate()[0].split('\n')
        yield 'libstdc++', {'versions': [x for x in stdversions if x.find('GLIBCXX') != -1]}

        stdversions = subprocess.Popen('lsb_release --all'.split(), stdout=subprocess.PIPE).communicate()[0].split('\n')
        yield 'lsb_release', dict([y.strip() for y in x.split(':', 1)] for x in stdversions if x.find(':') != -1)

        # /proc/cpuinfo
        yield 'cpuinfo', open('/proc/cpuinfo').read()

        # Environmental variables
        yield 'environ', os.environ


if __name__ == '__main__':
    hadoopy.run(Mapper)
