import hadoopy
import time

# Setup paths
data_path = 'hadoopy-test-data/%f/' % time.time()
input_path = data_path + 'wc-input-alice.tb'
output_path = data_path + 'wc-output-alice'
hadoopy.put('wc-input-alice.tb', input_path)

# Launch the job
hadoopy.launch_frozen(input_path, output_path, 'wc.py')

# Read the first KV pair
print(hadoopy.readtb(output_path).next())
