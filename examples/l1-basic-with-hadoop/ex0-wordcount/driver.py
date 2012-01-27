import hadoopy
import time
import os

# Setup paths
here = os.path.abspath(os.path.dirname(__file__))
data_path = 'hadoopy-test-data/%f/' % time.time()
input_path = data_path + 'wc-input-alice.tb'
output_path = data_path + 'wc-output-alice'

# Put the data from a local path onto HDFS

hadoopy.put(os.path.join(here, '..', '..', 'data' ,'wc-input-alice.tb'), input_path)

# Launch the job.  The wc.py script will be "frozen" (all dependencies are discovered using Pyinstaller).
# The cluster doesn't need Hadoopy, Python, or any other libraries for this to work (as long as Pyinstaller can find everything, if not there are simple things that you can do to fix it).
hadoopy.launch_frozen(input_path, output_path, 'wc.py')

# Analyze the output.  The output is an iterator of (word, count) where word is a string and count
# is an integer.
word_counts = dict(hadoopy.readtb(output_path))
for probe_word, expected_count in [('the', 1664), ('Alice', 221), ('tree', 3)]:
    print('word_counts[%s] = %d' % (probe_word, word_counts[probe_word]))
    assert expected_count == word_counts[probe_word]
