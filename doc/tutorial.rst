Tutorial
================================================
.. TODO Open with a discussion about the goal of the tutorial and mention that the jobs shown are all identities or null to teach purely Hadoopy, see the projects section for detailed examples.


Installing Hadoopy
------------------
The best way to get Hadoopy is off of the github_.

Github Clone ::

    git clone https://github.com/bwhite/hadoopy.git
    cd hadoopy
    sudo python setup.py install

PIP installation ::

    sudo pip install -e git+https://github.com/bwhite/hadoopy#egg=hadoopy

.. _github: http://github.com/bwhite/hadoopy

This guide assumes you have a functional Hadoop cluster with a supported version of Hadoop.  If you need help with this see the :doc:`cluster guide</clustersetup>`

.. TODO install guide
.. TODO Add cluster setup citation

Putting Data on HDFS
--------------------
As Hadoop is used to process large amounts of data, being able to easily put data on HDFS (Hadoop Distributed File System) is essential.  The primary way to do this using Hadoopy is with the hadoopy.writetb command which takes an iterator of key/value pairs and puts them in a SequenceFile encoded as TypedBytes.  This file can be used directly as input to Hadoopy jobs and they maintain their Python types due to the TypedBytes encoding.

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=datain_writetb.py"></script>

.. TODO Link to text

Anatomy of Hadoopy Jobs
-----------------------
Each job consists of a Map task and an optional Reduce task.  If a Reduce task is present then there is also an optional Combine task.  A job is often specified in a single Python (.py) file with a call to hadoopy.run.  The hadoopy.run command adds several command line options to the script and takes as arguments the map/reduce/combine classes/functions.  The function form is appropriate for small tasks that don't hold state (i.e., information is maintained between calls); however, when a task is large enough to spread across several functions, require initial setup, or hold state between jobs, the class form should be used.  All that is necessary to use a map/reduce/combine task is to provide it to the hadoopy.run function.  All tasks return an iterator of Key/Value pairs, the most common way to do this is by using the python "yield" statement to produce a generator.  The combiner and reducer take a key along with an iterator of values corresponding to that key as input.  Below are examples of map-only, map/reduce, and map/combine/reduce jobs that all act as an identify function (i.e., the same key/value pairs that are input are in the final output).

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=identity_func_m.py"></script>
    <script src="https://gist.github.com/3186206.js?file=identity_func_mr.py"></script>
    <script src="https://gist.github.com/3186206.js?file=identity_class_mrc.py"></script>

While using generators is the most common way to make Hadoopy tasks, as long as a task returns an iterator of Key/Value pairs or None (useful if the task doesn't output when it is called) it will work.

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=identity_func_m_list.py"></script>
    <script src="https://gist.github.com/3186206.js?file=null_func_m.py"></script>

The class form also provides an additional capability that is useful in more advanced design patterns.  A class can specify a "close" method that is called after all inputs are provided to the task.  This often leads to map/reduce tasks that return None when called, but produce meaningful output when close is called.  Below is an example of an identity Map task that buffers all the inputs and outputs them during close.  Note this is to demonstrate the concept and would generally be a bad idea as the Map tasks would run out of memory when given large inputs.  

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=identity_class_m_close.py"></script>


Running Jobs
--------------------
Now that we have a Hadoopy job, we would like to execute it on an existing Hadoop cluster.  To launch a job, hadoopy builds the necessary command line arguments to call the "hadoop" command with.  The command that is constructed is shown when the job is launched (depending on the logging level enabled).  The hadoopy.launch command requires an hdfs input, hdfs output, and script path (in order).  It sends the python script with the job and it is executed on the cluster.  That means that everything the job script needs in terms of Python version, Python packages (e.g., numpy), C libraries (e.g., lapack), and utilities (e.g., ffmpeg) must already reside on the server.  If this sounds difficult (i.e., ensuring that all machines have identical libraries/packages) or impossible (e.g., you have no admin access to the cluster) then please continue reading to the "hadoopy.launch_frozen" command.  Additional files (including .py files) can be included in the "files" keyword argument (they will all be placed in the local directory of the job).



.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=launch.py"></script>



.. TODO Link to cluster setup guide
.. TODO Explain launch and launch_frozen
.. TODO Explain using the command line to launch jobs
.. TODO Provide a link to local
.. TODO Provide a link to flow


Writing Jobs
-------------
While each job is different I'll describe a common process for designing and writing them.  This process is for an entire workflow which may consist of multiple jobs and intermingled client-side processing.  The first step is to identify what you are trying to do as a series of steps (very important), you then start by identifying parallelism.  Is your data entirely independent (i.e., embarrassingly parallel)?  If so then use a Map-only job.  Does your problem involve a "join"?  If so then use a Map/Reduce job.  It helps if you think in extremes about your data.  Maybe you are using a small test set now, what if you were using a TB of data?

One of the most important things to get comfortable with is what data should be input to a job, and what data should be included as side-data.  Side-data is data that each job has access to and doesn't come as input to the job.  This is important because it enables many ways of factoring your problem.  Something to watch out for is making things "too scalable" in that you are developing jobs that have constant memory and time requirements (i.e., O(1)) but end up not using your machines efficiently.  A warning sign is when the majority of your time is spent in the Shuffle phase (i.e., copying/sorting data before the Reducer runs), at that point you should consider if there is a way to utilize side-data, a combiner (with a preference for in-mapper combiners), or computation on the local machine to speed the task up.  Side-data may be a trained classifier (e.g., face detector), configuration parameters (e.g., number of iterations), and small data (e.g., normalization factor, cluster centers).

Four ways of providing side data (in recommended order) are

* Files that are copied to the local directory of your job (using the "files" argument in the launchers)
* Environmental variables accessibile through os.environ (using the "cmdenvs" argument in the launchers)
* Python scripts (can be stored as a global string, useful with launch_frozen as it packages up imported .py files)
* HDFS paths (using hadoopy.readtb)

.. TODO Link to benchmark

Getting data from HDFS
----------------------
After you've run your Hadoop jobs you'll eventually want to get something back from HDFS.  The most effective way of doing this in Hadoopy is using the hadoopy.readtb command which provides an iterator over Key/Value pairs in a SequenceFile.  Below is an example of how to read data of HDFS and store each key/value pair as a file with name as the key and value as the file (assumes unique keys).

.. raw:: html

    <script src="https://gist.github.com/3186206.js?file=dataout_readtb.py"></script>

.. TODO Link to text
