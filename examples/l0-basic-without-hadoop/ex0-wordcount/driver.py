"""Local wordcount driver

This is designed to be a first lesson in hadoopy.  It doesn't require Hadoop
as it uses the 'launch_local' method which emulates the behavior of running
programs on Hadoop using Hadoopy.
"""
import hadoopy
import os

# Setup paths
input_path = os.path.abspath('../../data/wc-input-alice.txt')

# Read input as an iterator of (line_num, line).  'line_num' is the key
# and line is the value; however, in wc.py only the value is used.  Notice that
# using a generator in this form produces the (key, value) pairs lazily.
def get_lines(fn):
    line_count = 0
    with open(fn) as fp:
        for line in fp:
            yield line_count, line[:-1]
            line_count += 1

# Launch the job.  Hadoopy provides 3 ways of launching a job.
# 1. launch: The job is run like a standard Hadoop streaming python script
# 2. launch_frozen: The script is first frozen using Pyinstaller (which packages up dependencies)
#    and then run.  This is the most common way to use Hadoopy as it avoid having to install
#    anything on the cluster including Python, dependencies, and your code.
# 3. launch_local: This is intended for unit tests, debugging, education, and very small jobs.
#    It emulates the behavior of launch/launch_frozen as close as possible but on the local
#    machine.  Read its docstring for compatibility and details.
#
# The first argument is the input, for launch_local it can use HDFS paths or an iterator
# of (key, value) pairs.  The second argument is the output, it can use an HDFS path or None
# if the output shouldn't be written to HDFS (as in this case).  The third argument is the script
# path.  The return value of launch_local is a dictionary (see its docstring), and we want 'output'
# which is an iterator of the output (key, value) pairs.
#
# By default Hadoopy talks to Hadoop Streaming using a simple serialization format called TypedBytes.
# The alternative is line oriented records like key0<tab>value0<newline>key1<tab>value1<newline> which
# are 1.) less efficient, 2.) more annoying to work with as everything has to be a string and that
# string can't contain <tab> or <newline> characters.
#
# Note that the types of the (key, value) pairs can be any serializable Python type when using the
# TypedBytes interface (recommended and default), they will be presented to your program in the same
# form they are provided.  All base types are serialized very efficiently and they fall back to Pickle
# for types not supported by TypedBytes.  If this is confusing, just know that you can input/output
# anything you can pickle and Hadoopy does things in a fast way.
output_kvs = hadoopy.launch_local(get_lines(input_path), None, 'wc.py')['output']

# Analyze the output.  The output is an iterator of (word, count) where word is a string and count
# is an integer.
word_counts = dict(output_kvs)
for probe_word, expected_count in [('the', 1664), ('Alice', 221), ('tree', 3)]:
    print('word_counts[%s] = %d' % (probe_word, word_counts[probe_word]))
    assert expected_count == word_counts[probe_word]
