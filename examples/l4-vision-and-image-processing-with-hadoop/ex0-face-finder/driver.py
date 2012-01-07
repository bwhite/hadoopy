import hadoopy
import gzip
import subprocess
import time
import os
import cv2
import numpy as np
import cStringIO as StringIO
import Image

data_path = '../../data/'


def _run_face(fn):
    cur_time = time.time()
    hdfs_base_path = 'hadoopy-test-data/%f/' % cur_time
    print('Storing HDFS temp files and output in [%s]' % hdfs_base_path)
    in_path = hdfs_base_path + os.path.basename(fn)
    out_path = hdfs_base_path + 'out-' + os.path.basename(fn)
    cmd = 'hadoop fs -put %s %s' % (fn, in_path)
    subprocess.check_call(cmd.split())
    hadoopy.launch_frozen(in_path, out_path, 'face_finder.py', files=[data_path + 'haarcascade_frontalface_default.xml'])
    local_out = 'out-%f' % cur_time
    try:
        os.makedirs(local_out)
    except OSError:
        pass
    print('Storing local output in [%s]' % local_out)
    for num, (image_name, (image_data, faces)) in enumerate(hadoopy.readtb(out_path)):
        image = np.asarray(Image.open(StringIO.StringIO(image_data)))
        for (x, y, w, h), n in faces:
            cv2.rectangle(image, (x, y), (x + w, y + h), (0, 0, 255), 3)
        cv2.imwrite('%s/img%.8d.jpg' % (local_out, num), image[:, :, ::-1].copy())


with open(data_path + 'haarcascade_frontalface_default.xml', 'w') as fp:
    o = gzip.GzipFile(data_path + 'haarcascade_frontalface_default.xml.gz').read()
    fp.write(o)
_run_face(data_path + '/face_finder-input-voctrainpart.tb')
