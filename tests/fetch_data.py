import os
import hashlib
import urllib
import gzip


def load_from_s3(path, md5hash):
    name = os.path.basename(path)
    download = not os.path.exists(path)
    if os.path.exists(path) and md5hash:
        with open(path) as fp:
            if hashlib.md5(fp.read()).hexdigest() != md5hash:
                download = True
    if download:
        url = 'http://dv-data.s3.amazonaws.com/%s' % name
        print('Downloading [%s]' % url)
        data = urllib.urlopen(url).read()
        with open(path, 'w') as fp:
            if md5hash:
                assert(md5hash == hashlib.md5(data).hexdigest())
            fp.write(data)


def main():
    if not os.path.exists('../examples/data/haarcascade_frontalface_default.xml.gz'):
        fp = open('haarcascade_frontalface_default.xml', 'w')
        o = gzip.GzipFile('../examples/data/haarcascade_frontalface_default.xml.gz').read()
        fp.write(o)
        fp.close()
    load_from_s3('../examples/data/face_finder-input-voctrainpart.tb', 'dbc50c02103221a499fc7cc77a5b61e9')

if __name__ == '__main__':
    main()
