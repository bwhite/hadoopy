Local Hadoop Execution
======================

.. TODO Explain local execution

Mixing Local and Hadoop Computation
-----------------------------------
Using hadoopy.readtb along with hadoopy.writetb allows for trivial integration of local computation with Hadoop computation.  An architecture design pattern that I've found useful is to have your job launcher machine to have a very large amount of memory (compared to the cluster nodes).  This is useful because there are jobs that can be written in a constant memory formulation, but use excessive disk/network IO (e.g., training very large classifiers) and can be performed once on this node and then the result can be used by the Hadoop nodes.  A mock example of this is shown below.

.. TODO Example

Launch Local (execute jobs w/o Hadoop)
--------------------------------------
.. TODO
