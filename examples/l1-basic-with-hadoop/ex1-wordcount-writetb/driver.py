import hadoopy
import time

# Setup paths
data_path = 'hadoopy-test-data/%f/' % time.time()
input_path = data_path + 'wc-input'
output_path = data_path + 'wc-output'

# Write data to HDFS in the form of (term #, term)
input_data = enumerate('Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industrys standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.'.split())
hadoopy.writetb(input_path, input_data)

# Launch the job
hadoopy.launch_frozen(input_path, output_path, 'wc.py')

# Read the first KV pair
word_counts = dict(hadoopy.readtb(output_path))
for probe_word, expected_count in [('the', 6), ('Lorem', 4), ('of', 4)]:
    print('word_counts[%s] = %d' % (probe_word, word_counts[probe_word]))
    assert expected_count == word_counts[probe_word]
