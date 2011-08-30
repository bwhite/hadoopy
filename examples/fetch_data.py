import os
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

load_from_umiacs('data/face_finder-input-voctrainpart.tb', 'dbc50c02103221a499fc7cc77a5b61e9')
