Hadoopy: Python wrapper for Hadoop using Cython
================================================

..  toctree::
    :maxdepth: 2

    tutorial
    projects
    api
    internals
    benchmark
    flow
    helper

Visit https://github.com/bwhite/hadoopy/ for the source.

Relevant blog posts

* http://brandynwhite.com/hadoopy-cython-based-mapreduce-library-for-py

About
---------------------------
Hadoopy is a Python wrapper for Hadoop Streaming written in Cython.  It is simple, fast, and readily hackable.  It has been tested on 700+ node clusters.  The goals of Hadoopy are

* Similar interface as the Hadoop API (design patterns usable between Python/Java interfaces)
* General compatibility with dumbo to allow users to switch back and forth
* Usable on Hadoop clusters without Python or admin access
* Fast conversion and processing
* Stay small and well documented
* Be transparent with what is going on
* Handle programs with complicated .so's, ctypes, and extensions
* Code written for hack-ability
* Simple HDFS access (e.g., reading, writing, ls)
* Support (and not replicate) the greater Hadoop ecosystem (e.g., Oozie, whirr)

Killer Features (unique to Hadoopy)

* Automated job parallelization 'auto-oozie' available in the hadoopy flow_ project (maintained out of branch)
* Local execution of unmodified MapReduce job with launch_local
* Read/write sequence files of TypedBytes directly to HDFS from python (readtb, writetb)
* Allows printing to stdout and stderr in Hadoop tasks without causing problems (uses the 'pipe hopping' technique, both are available in the task's stderr)
* Works on clusters without any extra installation, Python, or any Python libraries (uses Pyinstaller that is included in this source tree)

.. _flow: https://github.com/bwhite/hadoopy_flow

Additional Features

* Works on OS X
* Critical path is in Cython
* Simple HDFS access (readtb and ls) inside Python, even inside running jobs
* Unit test interface
* Reporting using status and counters (and print statements! no need to be scared of them in Hadoopy)
* Supports design patterns in the Lin&Dyer book_
* Typedbytes support (very fast)
* Oozie support

.. _book: http://www.umiacs.umd.edu/~jimmylin/book.html
