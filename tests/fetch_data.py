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
    if not os.path.exists('haarcascade_frontalface_default.xml'):
        fp = open('haarcascade_frontalface_default.xml', 'w')
        o = gzip.GzipFile('../examples/data/haarcascade_frontalface_default.xml.gz').read()
        fp.write(o)
        fp.close()
    load_from_s3('../examples/data/test_images.tb', '935e4408f1416532a9aa5a6ac459e66b')

if __name__ == '__main__':
    main()
