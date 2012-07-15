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
        stdversions = subprocess.Popen('strings /usr/lib/libstdc++.so.6'.split(), stdout=subprocess.PIPE).communicate()[0]
        yield 'libstdc++', {'versions': [x for x in stdversions if x.find('LIBCXX') != -1]}

        # /proc/cpuinfo
        yield 'cpuinfo', open('/proc/cpuinfo').read()

        # Environmental variables
        yield 'environ', os.environ


if __name__ == '__main__':
    hadoopy.run(Mapper)
