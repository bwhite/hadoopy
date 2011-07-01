import hadoopy
import time
out = '_hadoopy_test/local_test/%f' % time.time()
hadoopy.launch_local('/user/brandyn/flickr_hash_data', out, 'local.py', max_input=1000, reducer=None, cmdenvs=['TEST_ENV=10'],
                     files=['mytest_dir/test_file'])
out = hadoopy.launch_local(((1000 * 'a', 10000000 * 'b') for x in range(100)), None, 'local.py', max_input=10000, reducer=None, cmdenvs=['TEST_ENV=10'],
                           files=['mytest_dir/test_file'])
