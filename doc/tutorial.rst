Hadoopy: Tutorial
================================================

Putting Data on Hadoop
--------------------------------
As Hadoop is used to process large amounts of data, being able to easily put data on HDFS (Hadoop Distributed File System) is essential.  The primary way to do this using Hadoopy is with the hadoopy.writetb command which takes an iterator of key/value pairs and puts them in a SequenceFile encoded as TypedBytes.  This file can be used directly as input to Hadoopy jobs and they maintain their Python types due to the TypedBytes encoding.

.. raw:: html

    <script src="https://gist.github.com/3182286.js?file=writetb.py"></script>

As Hadoopy jobs can take pure text as input, another option is to make a large line-oriented textfile.  The main drawback of this is that you have to do your own encoding (especially avoiding using newline/tab characters in binary data).  A conservative way to do this is to pick any encoding you want (e.g., JSON, Python Pickles, Protocol Buffers, etc.) and base64 encode the result.

.. raw:: html

    <script src="https://gist.github.com/3182434.js?file=datain_text.py"></script>
