import hadoopy
import time
import shutil
import os

data_path = '../../data/'
local_out = 'out/'

def _run_haystack(fn, script_name):
    cur_time = time.time()
    hdfs_base_path = 'hadoopy-test-data/%f/' % cur_time
    print('Storing HDFS temp files and output in [%s]' % hdfs_base_path)
    in_path = hdfs_base_path + os.path.basename(fn)
    out_path = hdfs_base_path + 'out-' + os.path.basename(fn)
    hadoopy.put(fn, in_path)
    print('Launching job [%s]' % script_name)
    hadoopy.launch_frozen(in_path, out_path, script_name, files=[data_path + 'target.jpg'])
    print('Storing local output in [%s]' % local_out)
    for num, (image_name, image_data) in enumerate(hadoopy.readtb(out_path)):
        open('%s%s-img%.8d-%s.jpg' % (local_out, script_name, num, image_name), 'w').write(image_data)

try:
    os.makedirs(local_out)
except OSError:
    pass
shutil.copy(data_path + 'target.jpg', local_out + 'target.jpg')
_run_haystack(data_path + '/test_images.tb', 'haystack.py')
_run_haystack(data_path + '/test_images.tb', 'haystack_imc.py')
