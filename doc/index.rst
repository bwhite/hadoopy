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

The rest of the nodes were cleaned up the same way.  We modify the command, note that we now get the output from freeze at the top ::
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


Most Hadoop streaming programs have to meticulously avoid printing to stdout as it will interfere with the connection to Hadoop streaming.  Hadoopy uses a technique I refer to as 'pipe hopping' where a launcher remaps the stdin/stdout of the client program to be null and stderr respectively, and communication happens over file descriptors which correspond to the true stdout/stdin that Hadoop streaming communicates with.  This is transparent to the user but the end result is more useful error messages when exceptions are thrown (as opposed to generic Java errors) and the ability to use print statements like normal.

That's a quick tour of Hadoopy.

API
---

..  function:: hadoopy.run(mapper=None[, reducer=None, combiner=None, **kw])
    Hadoopy entrance function

    This is to be called in all Hadoopy job's.  Handles arguments passed in,
    calls the provided functions with input, and stores the output.

    TypedBytes are used if the following is True
    os.environ['stream_map_input'] == 'typedbytes'

    It is *highly* recommended that TypedBytes be used for all non-trivial
    tasks.  Keep in mind that the semantics of what you can safely emit from
    your functions is limited when using Text (i.e., no \\t or \\n).  You can use
    the base64 module to ensure that your output is clean.

    If the HADOOPY_CHDIR environmental variable is set, this will immediately
    change the working directory to the one specified.  This is useful if your
    data is provided in an archive but your program assumes it is in that
    directory.

    As hadoop streaming relies on stdin/stdout/stderr for communication,
    anything that outputs on them in an unexpected way (especially stdout) will
    break the pipe on the Java side and can potentially cause data errors.  To
    fix this problem, hadoopy allows file descriptors (integers) to be provided
    to each task.  These will be used instead of stdin/stdout by hadoopy.  This
    is designed to combine with the 'pipe' command.

    To use the pipe functionality, instead of using
    `your_script.py map` use `your_script.py pipe map`
    which will call the script as a subprocess and use the read_fd/write_fd
    command line arguments for communication.  This isolates your script and
    eliminates the largest source of errors when using hadoop streaming.

    The pipe functionality has the following semantics
    stdin: Always an empty file
    stdout: Redirected to stderr (which is visible in the hadoop log)
    stderr: Kept as stderr
    read_fd: File descriptor that points to the true stdin
    write_fd: File descriptor that points to the true stdout

    | **Command Interface**
    | The command line switches added to your script (e.g., script.py) are

    python script.py *map* (read_fd) (write_fd)
        Use the provided mapper, optional read_fd/write_fd.
    python script.py *reduce* (read_fd) (write_fd)
        Use the provided reducer, optional read_fd/write_fd.
    python script.py *combine* (read_fd) (write_fd)
        Use the provided combiner, optional read_fd/write_fd.
    python script.py *freeze* <tar_path> <-Z add_file0 -Z add_file1...>
        Freeze the script to a tar file specified by <tar_path>.  The extension
        may be .tar or .tar.gz.  All files are placed in the root of the tar.
        Files specified with -Z will be added to the tar root.
    python script.py info
        Prints a json object containing 'tasks' which is a list of tasks which
        can include 'map', 'combine', and 'reduce'.  Also contains 'doc' which is
        the provided documentation through the doc argument to the run function.
        The tasks correspond to provided inputs to the run function.

    | **Specification of mapper/reducer/combiner** 
    | Input Key/Value Types
    |     For TypedBytes/SequenceFileInputFormat, the Key/Value are the decoded TypedBytes
    |     For TextInputFormat, the Key is a byte offset (int) and the Value is a line without the newline (string)
    |
    | Output Key/Value Types
    |     For TypedBytes, anything Pickle-able can be used
    |     For Text, types are converted to string.  Note that neither may contain \\t or \\n as these are used in the encoding.  Output is key\\tvalue\\n
    |
    | Expected arguments
    |     mapper(key, value) or mapper.map(key, value)
    |     reducer(key, values) or reducer.reduce(key, values)
    |     combiner(key, values) or combiner.reduce(key, values)
    |
    | Optional methods
    |     func.configure(): Called before any input read.  Returns None.
    |     func.close():  Called after all input read.  Returns None or Iterator of (key, value)
    |
    | Expected return
    |     None or Iterator of (key, value)

    :param mapper: Function or class following the above spec
    :param reducer: Function or class following the above spec
    :param combiner: Function or class following the above spec
    :param doc: If specified, on error print this and call sys.exit(1)
    :rtype: True on error, else False (may not return if doc is set and
        there is an error)


..  function:: hadoopy.status(msg[, err=None])

    Output a status message that is displayed in the Hadoop web interface

    The status message will replace any other, if you want to append you must
    do this yourself.

    :param msg: String representing the status message
    :param err: Func that outputs a string, if None then sys.stderr.write is used (default None)

..  function:: hadoopy.counter(group, counter[, amount=1, err=None])

    Output a counter update that is displayed in the Hadoop web interface

    Counters are useful for quickly identifying the number of times an error
    occurred, current progress, or coarse statistics.

    :param group: Counter group
    :param counter: Counter name
    :param amount: Value to add (default 1)
    :param err: Func that outputs a string, if None then sys.stderr.write is used (default None)

..  function:: hadoopy.launch(in_name, out_name, script_path[, mapper=True, reducer=True, combiner=False, partitioner=False, files=(), jobconfs=(), cmdenvs=(), copy_script=True, hstreaming=None, name=None, use_typedbytes=True, use_seqoutput=True, use_autoinput=True, pretend=False, add_python=True, config=None, **kw])
    
    Run Hadoop given the parameters

    :param in_name: Input path (string or list)
    :param out_name: Output path
    :param script_path: Path to the script (e.g., script.py)
    :param mapper: If True, the mapper is "script.py map".  If string, the mapper is the value
    :param reducer: If True (default), the reducer is "script.py reduce".  If string, the reducer is the value
    :param combiner: If True, the combiner is "script.py combine" (default False).  If string, the combiner is the value
    :param partitioner: If True, the partitioner is the value.
    :param copy_script: If True, the script is added to the files list.
    :param files: Extra files (other than the script) (string or list).  NOTE: Hadoop copies the files into working directory
    :param jobconfs: Extra jobconf parameters (string or list)
    :param cmdenvs: Extra cmdenv parameters (string or list)
    :param hstreaming: The full hadoop streaming path to call.
    :param use_typedbytes: If True (default), use typedbytes IO.
    :param use_seqoutput: True (default), output sequence file. If False, output is text.
    :param use_autoinput: If True (default), sets the input format to auto.
    :param pretend: If true, only build the command and return.
    :param add_python: If true, use 'python script_name.py'
    :param config: If a string, set the hadoop config path
    :rtype: The hadoop command called.
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.


..  function:: hadoopy.launch_frozen(in_name, out_name, script_path[, mapper=True, reducer=True, combiner=False, partitioner=False, files=(), jobconfs=(), cmdenvs=(), copy_script=True, hstreaming=None, name=None, use_typedbytes=True, use_seqoutput=True, use_autoinput=True, pretend=False, add_python=True, config=None, verbose=False, **kw])

    Freezes a script and then launches it.

    :param in_name: Input path (string or list)
    :param out_name: Output path
    :param script_path: Path to the script (e.g., script.py)
    :param mapper: If True, the mapper is "script.py map".  If string, the mapper is the value
    :param reducer: If True (default), the reducer is "script.py reduce".  If string, the reducer is the value
    :param combiner: If True, the combiner is "script.py combine" (default False).  If string, the combiner is the value
    :param partitioner: If True, the partitioner is the value.
    :param copy_script: If True, the script is added to the files list.
    :param files: Extra files (other than the script) (string or list).  NOTE: Hadoop copies the files into working directory
    :param jobconfs: Extra jobconf parameters (string or list)
    :param cmdenvs: Extra cmdenv parameters (string or list)
    :param hstreaming: The full hadoop streaming path to call.
    :param use_typedbytes: If True (default), use typedbytes IO.
    :param use_seqoutput: True (default), output sequence file. If False, output is text.
    :param use_autoinput: If True (default), sets the input format to auto.
    :param pretend: If true, only build the command and return.
    :param add_python: If true, use 'python script_name.py'
    :param config: If a string, set the hadoop config path
    :param verbose: If true, output to stdout all command results.
    :rtype: The hadoop command called.
    :raises: subprocess.CalledProcessError: Hadoop or freeze error.
    :raises: OSError: Hadoop streaming or freeze not found.

..  function:: hadoopy.ls(path)

    List files on HDFS.

    :param path: A string (potentially with wildcards).
    :rtype: A list of strings representing HDFS paths.
    :raises: IOError: An error occurred listing the directory (e.g., not available).


..  autofunction:: hadoopy.readtb(path[, procs=10])

    Read typedbytes sequence files on HDFS (with optional compression).

    :param path: A string (potentially with wildcards).
    :param procs: Number of processes to use.
    :rtype: An iterator of key, value pairs.
    :raises: IOError: An error occurred listing the directory (e.g., not available).


..  autoclass:: hadoopy.GroupedValues
    :members:
..  autoclass:: hadoopy.Test
    :members:
..  autoclass:: hadoopy.TypedBytesFile
    :members:
