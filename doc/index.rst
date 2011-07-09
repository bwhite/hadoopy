..  Hadoopy documentation master file, created by
    sphinx-quickstart on Sat Jan 15 20:41:41 2011.
    You can adapt this file completely to your liking, but it should at least
    contain the root `toctree` directive.

Hadoopy: Python wrapper for Hadoop using Cython
================================================


..  toctree::
    :maxdepth: 2

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


Benchmark
---------

The majority of the time spent by Hadoopy (and Dumbo) is in the TypedBytes conversion code.  This is a simple binary serialization format that covers standard types with the ability to use Pickle for types not natively supported.  We generate a large set of test vectors (using the tb_make_test.py_ script), that have primatives, containers, and a uniform mix (GrabBag).  The idea is that by factoring out the types, we can easily see where optimization is needed.  Each element is read from stdin, then written to stdout.  Outside of the timing all of the values are compared to ensure that the final written values are the same.  Four methods are compared:  Hadoopy TypedBytes (speed_hadoopy.py_), Hadoopy TypedBytes file interface (speed_hadoopyfp.py_), TypedBytes_ (speed_tb.py_), and cTypedBytes_ (speed_tbc.py_).  All columns are in seconds except for ratio.  The ratio is min(TB, cTB) / HadoopyFP (e.g., 7 means HadoopyFP is 7 times faster).

.. _tb_make_test.py: https://github.com/bwhite/hadoopy/blob/master/play/tb_make_test.py
.. _speed_hadoopy.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_hadoopy.py
.. _speed_hadoopyfp.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_hadoopyfp.py
.. _speed_tbc.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_tbc.py
.. _speed_tb.py: https://github.com/bwhite/hadoopy/blob/master/play/speed_tb.py
.. _TypedBytes:  https://github.com/klbostee/typedbytes
.. _cTypedBytes: https://github.com/klbostee/ctypedbytes

+-----------------+---------+---------+---------+---------+---------+
|Filename         |Hadoopy  |HadoopyFP|TB       |cTB      |Ratio    |
+=================+=========+=========+=========+=========+=========+
|   double_100k.tb| 0.148790| 0.119961| 0.904720| 0.993845| 7.541784|
+-----------------+---------+---------+---------+---------+---------+
|    float_100k.tb| 0.145637| 0.118920| 0.883124| 0.992447| 7.426198|
+-----------------+---------+---------+---------+---------+---------+
|       gb_100k.tb| 4.638573| 4.011934|25.577765|16.515563| 4.116609|
+-----------------+---------+---------+---------+---------+---------+
|     bool_100k.tb| 0.171327| 0.150975| 0.942188| 0.542741| 3.594907|
+-----------------+---------+---------+---------+---------+---------+
|       dict_50.tb| 0.394323| 0.364878| 1.649921| 1.225979| 3.359970|
+-----------------+---------+---------+---------+---------+---------+
|      tuple_50.tb| 0.370037| 0.413579| 1.546317| 1.241491| 3.001823|
+-----------------+---------+---------+---------+---------+---------+
|     byte_100k.tb| 0.183307| 0.164549| 0.894184| 0.487520| 2.962767|
+-----------------+---------+---------+---------+---------+---------+
|       list_50.tb| 0.355870| 0.370738| 1.529233| 1.092422| 2.946614|
+-----------------+---------+---------+---------+---------+---------+
|      int_100k.tb| 0.234842| 0.193235| 0.922423| 0.526160| 2.722903|
+-----------------+---------+---------+---------+---------+---------+
|     long_100k.tb| 0.761289| 0.640638| 1.727951| 1.957162| 2.697234|
+-----------------+---------+---------+---------+---------+---------+
| bytes_100_10k.tb| 0.069889| 0.069375| 0.147470| 0.096838| 1.395862|
+-----------------+---------+---------+---------+---------+---------+
|string_100_10k.tb| 0.106642| 0.104784| 0.157907| 0.106571| 1.017054|
+-----------------+---------+---------+---------+---------+---------+
|string_10k_10k.tb| 6.392013| 6.527343| 6.494607| 6.949912| 0.994985|
+-----------------+---------+---------+---------+---------+---------+
| bytes_10k_10k.tb| 3.073718| 3.123196| 3.039668| 3.100858| 0.973256|
+-----------------+---------+---------+---------+---------+---------+
| string_1k_10k.tb| 0.742198| 0.719119| 0.686382| 0.676537| 0.940786|
+-----------------+---------+---------+---------+---------+---------+
|  bytes_1k_10k.tb| 0.379785| 0.370314| 0.329728| 0.339387| 0.890401|
+-----------------+---------+---------+---------+---------+---------+
|     gb_single.tb| 0.045760| 0.042701| 0.038656| 0.034925| 0.817896|
+-----------------+---------+---------+---------+---------+---------+

Example - Hello Wordcount!
---------------------------
Python Source (fully documented version in wc.py_) ::

    """Hadoopy Wordcount Demo"""
    import hadoopy

    def mapper(key, value):
        for word in value.split():
            yield word, 1

    def reducer(key, values):
        accum = 0
        for count in values:
            accum += int(count)
        yield key, accum

    if __name__ == "__main__":
        hadoopy.run(mapper, reducer, doc=__doc__)

.. _wc.py: https://github.com/bwhite/hadoopy/blob/master/tests/wc.py

Command line test (run without args, it prints the docstring and quits because of doc=__doc__) ::

    $ python wc.py
    Hadoopy Wordcount Demo

Command line test (map) ::

    $ echo "a b a a b c" | python wc.py map
    a    1
    b    1
    a    1
    a    1
    b    1
    c    1

Command line test (map/sort) ::

    $ echo "a b a a b c" | python wc.py map | sort
    a    1
    a    1
    a    1
    b    1
    b    1
    c    1

Command line test (map/sort/reduce) ::

    $ echo "a b a a b c" | python wc.py map | sort | python wc.py reduce
    a    3
    b    2
    c    1

Here are a few test files ::

    $ hadoop fs -ls playground/
    Found 3 items
    -rw-r--r--   2 brandyn supergroup     259835 2011-01-17 18:56 /user/brandyn/playground/wc-input-alice.tb
    -rw-r--r--   2 brandyn supergroup     167529 2011-01-17 18:56 /user/brandyn/playground/wc-input-alice.txt
    -rw-r--r--   2 brandyn supergroup      60638 2011-01-17 18:56 /user/brandyn/playground/wc-input-alice.txt.gz

We can also do this in Python

    >>> import hadoopy
    >>> hadoopy.ls('playground/')
    ['/user/brandyn/playground/wc-input-alice.tb', '/user/brandyn/playground/wc-input-alice.txt', '/user/brandyn/playground/wc-input-alice.txt.gz']

Lets put wc-input-alice.txt through the word counter using Hadoop.  Each node in the cluster has Hadoopy installed (later we will show that it isn't necessary with launch_frozen).  Note that it is using typedbytes, SequenceFiles, and the AutoInputFormat by default.

    >>> out = hadoopy.launch('playground/wc-input-alice.txt', 'playground/out/', 'wc.py')
    /\----------Hadoop Output----------/\
    hadoopy: Running[hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2+737.jar -output playground/out/ -input playground/wc-input-alice.txt -mapper "python wc.py map" -reducer "python wc.py reduce" -file wc.py -jobconf mapred.job.name=python wc.py -io typedbytes -outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat -    inputformat AutoInputFormat]
    11/01/17 20:22:31 WARN streaming.StreamJob: -jobconf option is deprecated, please use -D instead.
    packageJobJar: [wc.py, /var/lib/hadoop-0.20/cache/brandyn/hadoop-unjar464849802654976085/] [] /tmp/streamjob1822202887260165136.jar tmpDir=null
    11/01/17 20:22:32 INFO mapred.FileInputFormat: Total input paths to process : 1
    11/01/17 20:22:32 INFO streaming.StreamJob: getLocalDirs(): [/var/lib/hadoop-0.20/cache/brandyn/mapred/local]
    11/01/17 20:22:32 INFO streaming.StreamJob: Running job: job_201101141644_0723
    11/01/17 20:22:32 INFO streaming.StreamJob: To kill this job, run:
    11/01/17 20:22:32 INFO streaming.StreamJob: /usr/lib/hadoop-0.20/bin/hadoop job  -Dmapred.job.tracker=node.com:8021 -kill job_201101141644_0723
    11/01/17 20:22:32 INFO streaming.StreamJob: Tracking URL: http://node.com:50030/jobdetails.jsp?jobid=job_201101141644_0723
    11/01/17 20:22:33 INFO streaming.StreamJob:  map 0%  reduce 0%
    11/01/17 20:22:40 INFO streaming.StreamJob:  map 50%  reduce 0%
    11/01/17 20:22:41 INFO streaming.StreamJob:  map 100%  reduce 0%
    11/01/17 20:22:52 INFO streaming.StreamJob:  map 100%  reduce 100%
    11/01/17 20:22:55 INFO streaming.StreamJob: Job complete: job_201101141644_0723
    11/01/17 20:22:55 INFO streaming.StreamJob: Output: playground/out/
    \/----------Hadoop Output----------\/

Return value is a dictionary of the command's run, key/value iterator of the output (lazy evaluated), and other useful intermediate values.

Lets see what the output looks like.

    >>> out = list(hadoopy.readtb('playground/out'))
    >>> out[:10]
    [('*', 60), ('-', 7), ('3', 2), ('4', 1), ('A', 8), ('I', 260), ('O', 1), ('a', 662), ('"I', 7), ("'A", 9)]
    >>> out.sort(lambda x, y: cmp(x[1], y[1]))
    >>> out[-10:]
    [('was', 329), ('it', 356), ('in', 401), ('said', 416), ('she', 484), ('of', 596), ('a', 662), ('to', 773), ('and', 780), ('the', 1664)]

Note that the output is stored in SequenceFiles and each key/value is stored encoded as TypedBytes, by using readtb you don't have to worry about any of that (if the output was compressed it would also be decompressed here).  This can also be done inside of a job for getting additional side-data off of HDFS.

What if we don't want to install python, numpy, scipy, or your-custom-code-that-always-changes on every node in the cluster?  We have you covered there too.  I'll remove hadoopy from all nodes except for the one executing the job. ::

    $ sudo rm -r /usr/local/lib/python2.7/dist-packages/hadoopy*

Now it's gone

    >>> import hadoopy
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
    ImportError: No module named hadoopy

The rest of the nodes were cleaned up the same way.  We modify the command, note that we now get the output from freeze at the top

    >>> out = hadoopy.launch_frozen('playground/wc-input-alice.txt', 'playground/out_frozen/', 'wc.py')
    /\----------Hadoop Output----------/\
    hadoopy: Running[hadoop jar /hadoop-0.20.2+320/contrib/streaming/hadoop-streaming-0.20.2+320.jar -output playground/out_frozen/ -input playground/wc-input-alice.txt -mapper "_frozen/wc pipe map" -reducer "_frozen/wc pipe reduce" -jobconf "mapred.cache.archives=_hadoopy_temp/1310088192.511625/_frozen.tar#_frozen" -jobconf "mapreduce.job.cache.archives=_hadoopy_temp/1310088192.511625/_frozen.tar#_frozen" -jobconf mapred.job.name=wc -io typedbytes -outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat -inputformat AutoInputFormat]
    11/07/07 21:23:23 WARN streaming.StreamJob: -jobconf option is deprecated, please use -D instead.
    packageJobJar: [/tmp/hadoop/brandyn/hadoop-unjar12844/] [] /tmp/streamjob12845.jar tmpDir=null
    11/07/07 21:23:24 INFO mapred.FileInputFormat: Total input paths to process : 1
    11/07/07 21:23:24 INFO streaming.StreamJob: getLocalDirs(): [/scratch0/hadoop/mapred/local, /scratch1/hadoop/mapred/local, /scratch2/hadoop/mapred/local]
    11/07/07 21:23:24 INFO streaming.StreamJob: Running job: job_201107051032_0215
    11/07/07 21:23:24 INFO streaming.StreamJob: To kill this job, run:
    11/07/07 21:23:24 INFO streaming.StreamJob: /hadoop-0.20.2+320/bin/hadoop job  -Dmapred.job.tracker=node.com:8021 -kill job_201107051032_0215
    11/07/07 21:23:24 INFO streaming.StreamJob: Tracking URL: http://node.com:50030/jobdetails.jsp?jobid=job_201107051032_0215
    11/07/07 21:23:25 INFO streaming.StreamJob:  map 0%  reduce 0%
    11/07/07 21:23:31 INFO streaming.StreamJob:  map 100%  reduce 0%
    11/07/07 21:23:46 INFO streaming.StreamJob:  map 100%  reduce 100%
    11/07/07 21:23:49 INFO streaming.StreamJob: Job complete: job_201107051032_0215
    11/07/07 21:23:49 INFO streaming.StreamJob: Output: playground/out_frozen/
    \/----------Hadoop Output----------\/

And lets check the output

    >>> out = list(hadoopy.readtb('playground/out_frozen'))
    >>> out[:10]
    [('*', 60), ('-', 7), ('3', 2), ('4', 1), ('A', 8), ('I', 260), ('O', 1), ('a', 662), ('"I', 7), ("'A", 9)]
    >>> out.sort(lambda x, y: cmp(x[1], y[1]))
    >>> out[-10:]
    [('was', 329), ('it', 356), ('in', 401), ('said', 416), ('she', 484), ('of', 596), ('a', 662), ('to', 773), ('and', 780), ('the', 1664)]

We can also generate a tar of the frozen script (useful when working with Oozie).  Note the 'wc' is not wc.py, it is actually a self contained executable. ::

    $ python wc.py freeze wc.tar
    $ tar -tf wc.tar
    _codecs_cn.so
    readline.so
    strop.so
    cPickle.so
    time.so
    _collections.so
    operator.so
    zlib.so
    _codecs_jp.so
    grp.so
    _codecs_kr.so
    _codecs_hk.so
    _functools.so
    _json.so
    math.so
    libbz2.so.1.0
    libutil.so.1
    unicodedata.so
    array.so
    _bisect.so
    libz.so.1
    _typedbytes.so
    _random.so
    _main.so
    cStringIO.so
    _codecs_tw.so
    libncurses.so.5
    datetime.so
    _struct.so
    _weakref.so
    fcntl.so
    _heapq.so
    wc
    _io.so
    select.so
    _codecs_iso2022.so
    _locale.so
    itertools.so
    binascii.so
    bz2.so
    libpython2.7.so.1.0
    _multibytecodec.so

Lets open it up and try it out ::

    $ tar -xf wc.py
    $ ./wc
    Hadoopy Wordcount Demo
    $ python wc.py 
    Hadoopy Wordcount Demo
    $ hexdump -C wc | head -n5
    00000000  7f 45 4c 46 02 01 01 00  00 00 00 00 00 00 00 00  |.ELF............|
    00000010  02 00 3e 00 01 00 00 00  80 41 40 00 00 00 00 00  |..>......A@.....|
    00000020  40 00 00 00 00 00 00 00  50 04 16 00 00 00 00 00  |@.......P.......|
    00000030  00 00 00 00 40 00 38 00  09 00 40 00 1d 00 1c 00  |....@.8...@.....|
    00000040  06 00 00 00 05 00 00 00  40 00 00 00 00 00 00 00  |........@.......|

You can determine if a job provides map/reduce/combine functionality and get its documention by using 'info'.  This is also used internally by Hadoopy to autoatically enable/disable the reducer/combiner.  The task values are set when the corresponding slots in hadoopy.run are filled.

    >>> python wc.py info
    {"doc": "Hadoopy Wordcount Demo", "tasks": ["map", "reduce"]}

That's a quick tour of Hadoopy.

Pipe Hopping: Using Stdout/Stderr in Hadoopy Jobs
--------------------------------------------------

Hadoop streaming implements the standard Mapper/Reducer classes and simply opens 3 pipes to a streaming program (stdout, stderr, and stdin).  The first issue is how is data encoded?  The standard is to separate keys and values with a tab and each key/value pair with a newline; however, this is really a bad way to have to work as you have to ensure that your output never contains tabs or newlines.  Moreover, serializing everything to an escaped string is inefficient and tends to hurt interoperability of jobs as everyone has their own solution to encoding.  The solution (part of CDH2+) is to use TypedBytes which is an encoding format for basic types (int, float, dictionary, list, string, etc.) which is fast, standardized, and simple.  Hadoopy has its own implementation and it is particularly fast.

TypedBytes doesn't solve the issue of client code outputting to stdout, it actually makes it worse as the resulting output is interpreted as TypedBytes which can have very complex effects.  Most Hadoop streaming programs have to meticulously avoid printing to stdout as it will interfere with the connection to Hadoop streaming.  Hadoopy uses a technique I refer to as 'pipe hopping' where a launcher remaps the stdin/stdout of the client program to be null and stderr respectively, and communication happens over file descriptors which correspond to the true stdout/stdin that Hadoop streaming communicates with.  This is transparent to the user but the end result is more useful error messages when exceptions are thrown (as opposed to generic Java errors) and the ability to use print statements like normal.  This is a general solution to the problem and if other library writers (for python or other languages) would like a minimum working example of this technique I have one available.

This technique is on by default and can be disabled by passing pipe=False to the launch command of your choice.


Hadoopy Flow: Automatic Job-Level Parallization
-----------------------------------------------

Once you get past the wordcount examples and you have a few scripts you use regularly, the next level of complexity is managing a workflow of jobs.  The simplest way of doing this is to put a few sequential launch statements in a python script and run it.  This is fine for simple workflows but you miss out on two abilities: re-execution of previous workflows by re-using outputs (e.g., when tweaking one job in a flow) and parallel execution of jobs in a flow.  I've had some fairly complex flows and previously the best solution I could find was using Oozie_ with a several thousand line XML file.  Once setup, this ran jobs in parallel and re-execute the workflow by skipping previous nodes; however, it is another server you have to setup and making that XML file takes a lot of the fun out of using Python in the first place (it could be more code than your actual task).  While Hadoopy is fully compatible with Oozie, it certainly seems lacking for the kind of short turn-around scripts most users want to make.

In solving this problem, our goal was to avoid specifying the dependencies (often as a DAG) as they are inherent in the code itself.  Hadoopy Flow solves both of these problems by keeping track of all HDFS outputs your program intends to create and following your program order.  By doing this, if we see a 'launch' command we run it in a 'greenlet', note the output path of the job, and continue with the rest of the program.  If none of the job's inputs depend on any outputs that are pending (i.e., outputs that will materialize from previous jobs/hdfs commands) then we can safely start the job.  This is entirely safe because if the program worked before Hadoopy Flow, then it will work now as those inputs must exist as nothing prior to the job could have created it.  When a job completes, we notify dependent jobs/hdfs commands and if all of their inputs are available they are executed.  The same goes for HDFS commands such as readtb and writetb (most but not all HDFS commands are supported, see Hadoopy Flow for more info).  If you try to read from a file that another job will eventually output to but it hasn't finished yet, then the execution will block at that point until the necessary data is available.

So it sounds pretty magical, but it wouldn't be worth it if you have to rewrite all of your code.  To use Hadoopy Flow, all that you have to do is add 'import hadoopy_flow' before you import Hadoopy, and it will automatically parallelize your code.  It monkey patches Hadoopy (i.e., wraps the calls at run time) and the rest of your code can be unmodified.  All of the code is just a few hundred lines in one file, if you are familiar with greenlets then it might take you 10 minutes to fully understand it (which I recommend if you are going to use it regularly).

Re-execution is another important feature that Hadoopy Flow addresses and it does so trivially.  If after importing Hadoopy Flow you use 'hadoopy_flow.USE_EXISTING = True', then when paths already exist we simply skip the task/command that would have output to them.  This is useful if you run a workflow, a job crashes, fix the bug, delete the bad job's output, and re-run the workflow.  All previous jobs will be skipped and jobs that don't have their outputs on HDFS are executed like normal.  This simple addition makes iterative development using Hadoop a lot more fun and effective as tweaks generally happen at the end of the workflow and you can easily waste hours recomputing results or hacking your workflow apart to short circuit it.

.. _Oozie: http://yahoo.github.com/oozie/releases/3.0.0/


Job Driver API (Start Hadoop Jobs)
----------------------------------

..  autofunction:: hadoopy.launch(in_name, out_name, script_path[, partitioner=False, files=(), jobconfs=(), cmdenvs=(), copy_script=True, wait=True, hstreaming=None, name=None, use_typedbytes=True, use_seqoutput=True, use_autoinput=True, add_python=True, config=None, pipe=True, python_cmd="python", num_mappers=None, num_reducers=None, script_dir='', remove_ext=False, **kw])

..  autofunction:: hadoopy.launch_frozen(in_name, out_name, script_path[, frozen_tar_path=None, temp_path='_hadoopy_temp', partitioner=False, wait=True, files=(), jobconfs=(), cmdenvs=(), hstreaming=None, name=None, use_typedbytes=True, use_seqoutput=True, use_autoinput=True, add_python=True, config=None, pipe=True, python_cmd="python", num_mappers=None, num_reducers=None, **kw])

..  autofunction:: hadoopy.launch_local(in_name, out_name, script_path[, max_input=-1, files=(), cmdenvs=(), pipe=True, python_cmd='python', remove_tempdir=True, **kw])


Task API (used inside Hadoopy jobs)
-----------------------------------

..  autofunction:: hadoopy.run(mapper=None, reducer=None, combiner=None, **kw)
..  autofunction:: hadoopy.status(msg[, err=None])
..  autofunction:: hadoopy.counter(group, counter[, amount=1, err=None])

HDFS API (Usable locally and in Hadoopy jobs)
-----------

..  autofunction:: hadoopy.readtb(paths[, ignore_logs=True, num_procs=10])
..  autofunction:: hadoopy.writetb(path, kvs)
..  autofunction:: hadoopy.abspath(path)
..  autofunction:: hadoopy.ls(path)
..  autofunction:: hadoopy.get(hdfs_path, local_path)
..  autofunction:: hadoopy.put(local_path, hdfs_path)
..  autofunction:: hadoopy.rmr(path)
..  autofunction:: hadoopy.isempty(path)
..  autofunction:: hadoopy.isdir(path)
..  autofunction:: hadoopy.exists(path)

Testing API
-----------

..  autoclass:: hadoopy.Test
    :members:

Internal Classes
----------------

..  autoclass:: hadoopy.GroupedValues
    :members:

..  autoclass:: hadoopy.TypedBytesFile(fn=None, mode=None, read_fd=None, write_fd=None, flush_writes=False)
    :members:
