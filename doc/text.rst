Text Input
===========

When working with Hadoopy it is preferable to work with SequenceFiles encoded with TypedBytes; however, it can operate on raw text input too (e.g., put text data directly on HDFS).  This section explores the use of raw text with Hadoopy.  Note that this is distinct from Hadoop Streaming's communication method with Hadoopy (i.e., Text or TypedBytes), see :doc:`internals</internals>` for more discussion.

Putting Text Data on HDFS
-------------------------

As Hadoopy jobs can take pure text as input, another option is to make a large line-oriented textfile.  The main drawback of this is that you have to do your own encoding (especially avoiding using newline/tab characters in binary data).  A conservative way to do this is to pick any encoding you want (e.g., JSON, Python Pickles, Protocol Buffers, etc.) and base64 encode the result.  In the following example, we duplicate the behavior of the writetb method by base64 encoding the path and binary contents.  The two are distinguishable because of the tab separator and each pair is on a separate line.

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=datain_text.py"></script>
