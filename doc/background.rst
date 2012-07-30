Background
==========
This section contains links to relevent background material that is often useful when working with Hadoopy.

Python Generators
------------------
Python Generators are often used in Hadoopy jobs, below is an example of generators in Python.

.. raw:: html

    <script src="https://gist.github.com/3183545.js"> </script>


Using C Code in Python (Cython and Ctypes Example)
--------------------------------------------------
There are a variety of methods of calling C code from Python.  Two simple methods are using CTypes (a library that comes with python that can call functions in shared libraries) and Cython (a language that mixes Python and C that enables easily integrating them).  The recommended method is using Cython as it can build a self contained shared object (.so) which simplifies deployment as launch frozen is able to determine that it is being used and bring it along.  When using ctypes it can be difficult to ensure that the python executable can find the correct libraries and they must be manually included with the job.  The following example shows Python, Numpy, C (Ctypes), and C (Cython) usage.


.. raw:: html

    <script src="https://gist.github.com/3208889.js?file=demo.py"></script>

Cython
------
The core of Hadoopy is written in the Cython_ language which is effectively Python with types but that is compiled to C.  This enables the code to be easily maintained but perform at native speeds.

.. _Cython: http://cython.org/

MapReduce
---------------------
http://code.google.com/edu/parallel/mapreduce-tutorial.html

OpenCV
---------------------
http://docs.opencv.org/
http://opencv.willowgarage.com/wiki/

Hadoop
-------------------
http://hadoop.apache.org/
