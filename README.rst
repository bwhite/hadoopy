=======
hadoopy
=======

**Brandyn White** <bwhite@dappervision.com>

**Andrew Miller** <amiller@dappervision.com>  

**Source**  https://github.com/bwhite/hadoopy/

**Issues**  https://github.com/bwhite/hadoopy/issues

**Docs**    http://bwhite.github.com/hadoopy/

**IRC**: #hadoopy @ freenode.net

Requirements
------------
* python development headers (python-dev)
* build tools (build-essential)

Optional
--------
* cython (>=.13) (without this it falls back to the pregenerated .c files)

Features
--------
- oozie support
- Automated job parallelization 'auto-oozie' available in the hadoopy_flow project (maintained out of branch)
- typedbytes support (very fast)
- Local execution of unmodified MapReduce job with launch_local
- Read/write sequence files of TypedBytes directly to HDFS from python (readtb, writetb)
- Works on OS X
- Allows printing to stdout and stderr in Hadoop tasks without causing problems (uses the 'pipe hopping' technique, both are available in the task's stderr)
- critical path is in Cython
- works on clusters without any extra installation, Python, or any Python libraries (uses Pyinstaller that is included in this source tree)
- Simple HDFS access (readtb and ls) inside Python, even inside running jobs
- Unit test interface
- Reporting using status and counters (and print statements! no need to be scared of them in Hadoopy)
- Supports design patterns in the Lin/Dyer book (http://www.umiacs.umd.edu/~jimmylin/book.html)

Limitations
-----------
- Hadoop Local currently unsupported due to a bug in Hadoop's handling of the distributed cache in this mode.  Use psuedo-distributed instead for now.  (https://github.com/bwhite/hadoopy/issues/40)

Used in
-------
- A Case for Query by Image and Text Content: Searching Computer Help using Screenshots and Keywords (to appear in WWW'11)
- Web-Scale Computer Vision using MapReduce for Multimedia Data Mining (at KDD'10)
- Vitrieve: Visual Search engine
- Picarus: Hadoop computer vision toolbox

Ubuntu Install (others are similar)
-----------------------------------
::

  sudo apt-get install python-dev build-essential
  sudo python setup.py install


CentOS Install
--------------
::

  sudo yum groupinstall 'Development Tools'
  sudo yum install python-devel 
  pip install -e git+https://github.com/bwhite/hadoopy#egg=hadoopy
