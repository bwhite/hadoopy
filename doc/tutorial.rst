Hadoopy: Tutorial
================================================

Putting Data on HDFS
--------------------------------
As Hadoop is used to process large amounts of data, being able to easily put data on HDFS (Hadoop Distributed File System) is essential.  The primary way to do this using Hadoopy is with the hadoopy.writetb command which takes an iterator of key/value pairs and puts them in a SequenceFile encoded as TypedBytes.  This file can be used directly as input to Hadoopy jobs and they maintain their Python types due to the TypedBytes encoding.

.. raw:: html

    <script src="https://gist.github.com/3182286.js?file=writetb.py"></script>

As Hadoopy jobs can take pure text as input, another option is to make a large line-oriented textfile.  The main drawback of this is that you have to do your own encoding (especially avoiding using newline/tab characters in binary data).  A conservative way to do this is to pick any encoding you want (e.g., JSON, Python Pickles, Protocol Buffers, etc.) and base64 encode the result.  In the following example, we duplicate the behavior of the writetb method by base64 encoding the path and binary contents.  The two are distinguishable because of the tab separator and each pair is on a separate line.

.. raw:: html

    <script src="https://gist.github.com/3182434.js?file=datain_text.py"></script>

Anatomy of Hadoopy Jobs
-----------------------
Each job consists of a Map task and an optional Reduce task.  If a Reduce task is present then there is also an optional Combine task.  A job is often specified in a single Python (.py) file with a call to hadoopy.run.  The hadoopy.run command adds several command line options to the script and takes as arguments the map/reduce/combine classes/functions.  The function form is appropriate for small tasks that don't hold state (i.e., information is maintained between calls); however, when a task is large enough to spread across several functions, require initial setup, or hold state between jobs, the class form should be used.  All that is necessary to use a map/reduce/combine task is to provide it to the hadoopy.run function.  All tasks return an iterator of Key/Value pairs, the most common way to do this is by using the python "yield" statement to produce a generator.  The combiner and reducer take a key along with an iterator of values corresponding to that key as input.  Below are examples of map-only, map/reduce, and map/combine/reduce jobs that all act as an identify function (i.e., the same key/value pairs that are input are in the final output).

.. raw:: html

    <script src="https://gist.github.com/3182778.js"> </script>

While using generators is the most common way to make Hadoopy tasks, as long as a task returns an iterator of Key/Value pairs or None (useful if the task doesn't output when it is called) it will work.

.. raw:: html

    <script src="https://gist.github.com/3182990.js"> </script>

The class form also provides an additional capability that is useful in more advanced design patterns.  A class can specify a "close" method that is called after all inputs are provided to the task.  This often leads to map/reduce tasks that return None when called, but produce meaningful output when close is called.  Below is an example of an identity Map task that buffers all the inputs and outputs them during close.  Note this is to demonstrate the concept and would generally be a bad idea as the Map tasks would run out of memory when given large inputs.  

.. raw:: html

    <script src="https://gist.github.com/3183053.js?file=close.py"></script>


Running Jobs
--------------------



Writing Jobs
-------------
While each job is different I'll describe a common process for designing and writing them.  This process is for an entire workflow which may consist of multiple jobs and intermingled client-side processing.  The first step is to identify what you are trying to do as a series of steps (very important), you then start by identifying parallelism.  Is your data entirely independent (i.e., embarrassingly parallel)?  If so then use a Map-only job.  Does your problem involve a "join"?  If so then use a Map/Reduce job.  It helps if you think in extremes about your data.  Maybe you are using a small test set now, what if you were using a TB of data?

One of the most important things to get comfortable with is what data should be input to a job, and what data should be included as side-data.  Side-data is data that each job has access to and doesn't come as input to the job.  This is important because it enables many ways of factoring your problem.  Something to watch out for is making things "too scalable" in that you are developing jobs that have constant memory and time requirements (i.e., O(1)) but end up not using your machines efficiently.  A warning sign is when the majority of your time is spent in the Shuffle phase (i.e., copying/sorting data before the Reducer runs), at that point you should consider if there is a way to utilize side-data, a combiner (with a preference for in-mapper combiners), or computation on the local machine to speed the task up.  Side-data may be a trained classifier (e.g., face detector), configuration parameters (e.g., number of iterations), and small data (e.g., normalization factor, cluster centers).

Four ways of providing side data (in recommended order) are

* Files that are copied to the local directory of your job (using the "files" argument in the launchers)
* Environmental variables accessibile through os.environ (using the "cmdenvs" argument in the launchers)
* Python scripts (can be stored as a global string, useful with launch_frozen as it packages up imported .py files)
* HDFS paths (using hadoopy.readtb)

.. TODO Benchmark each method for a few common scenarios

Getting data from HDFS
----------------------
TODO hadoopy.readtb
