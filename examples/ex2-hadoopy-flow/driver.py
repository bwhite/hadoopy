try:
    import hadoopy_flow
except ImportError:
    raise ImportError('You need hadoopy_flow from https://github.com/bwhite/hadoopy_flow')
import hadoopy
import time

# Setup paths
data_path = 'hadoopy-test-data/%f/' % time.time()
input_path = data_path + 'input'
output_path_a = data_path + 'output_a'
output_path_b = data_path + 'output_b'
output_path_c = data_path + 'output_c'
output_path_d = data_path + 'output_d'

# Write data to HDFS in the form of (term #, term)
input_data = [(1, 5), ('dsfs', {'a': 3}), ([1, 2], 'sdflk')]  # Diverse KV input
hadoopy.writetb(input_path, input_data)

# Launch the jobs
hadoopy.launch_frozen(input_path, output_path_a, 'identity.py')
hadoopy.launch_frozen(input_path, output_path_b, 'identity.py')
hadoopy.launch_frozen(output_path_b, output_path_c, 'identity.py')
hadoopy.launch_frozen([input_path, output_path_a, output_path_b, output_path_c], output_path_d, 'identity.py')

# Read the first KV pair
print('KV Input[%s]' % str(hadoopy.readtb(input_path).next()))
print('KV Output a[%s]' % str(hadoopy.readtb(output_path_a).next()))
print('KV Output b[%s]' % str(hadoopy.readtb(output_path_b).next()))
print('KV Output c[%s]' % str(hadoopy.readtb(output_path_c).next()))
print('KV Output d[%s]' % str(hadoopy.readtb(output_path_d).next()))
