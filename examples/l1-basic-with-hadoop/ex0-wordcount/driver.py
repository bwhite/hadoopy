import hadoopy
import time

# Setup paths
data_path = 'hadoopy-test-data/%f/' % time.time()
input_path = data_path + 'wc-input-alice.tb'
output_path = data_path + 'wc-output-alice'
hadoopy.put('../../data/wc-input-alice.tb', input_path)

# Launch the job
hadoopy.launch_frozen(input_path, output_path, 'wc.py')

# Analyze the output.  The output is an iterator of (word, count) where word is a string and count
# is an integer.
word_counts = dict(hadoopy.readtb(output_path))
for probe_word in ['the', 'Alice', 'tree']:
    print('word_counts[%s] = %d' % (probe_word, word_counts[probe_word]))
