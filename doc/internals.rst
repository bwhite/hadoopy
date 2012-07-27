Hadoopy Internals
=================

Pipe Hopping: Using Stdout/Stderr in Hadoopy Jobs
--------------------------------------------------

Hadoop streaming implements the standard Mapper/Reducer classes and simply opens 3 pipes to a streaming program (stdout, stderr, and stdin).  The first issue is how is data encoded?  The standard is to separate keys and values with a tab and each key/value pair with a newline; however, this is really a bad way to have to work as you have to ensure that your output never contains tabs or newlines.  Moreover, serializing everything to an escaped string is inefficient and tends to hurt interoperability of jobs as everyone has their own solution to encoding.  The solution (part of CDH2+) is to use TypedBytes which is an encoding format for basic types (int, float, dictionary, list, string, etc.) which is fast, standardized, and simple.  Hadoopy has its own implementation and it is particularly fast.

TypedBytes doesn't solve the issue of client code outputting to stdout, it actually makes it worse as the resulting output is interpreted as TypedBytes which can have very complex effects.  Most Hadoop streaming programs have to meticulously avoid printing to stdout as it will interfere with the connection to Hadoop streaming.  Hadoopy uses a technique I refer to as 'pipe hopping' where a launcher remaps the stdin/stdout of the client program to be null and stderr respectively, and communication happens over file descriptors which correspond to the true stdout/stdin that Hadoop streaming communicates with.  This is transparent to the user but the end result is more useful error messages when exceptions are thrown (as opposed to generic Java errors) and the ability to use print statements like normal.  This is a general solution to the problem and if other library writers (for python or other languages) would like a minimum working example of this technique I have one available.

This technique is on by default and can be disabled by passing pipe=False to the launch command of your choice.

Wordcount Job
-------------
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


Launching a job
---------------

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

Launching a job (Frozen Script)
-------------------------------

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

Script Info
-----------
You can determine if a job provides map/reduce/combine functionality and get its documention by using 'info'.  This is also used internally by Hadoopy to automatically enable/disable the reducer/combiner.  The task values are set when the corresponding slots in hadoopy.run are filled.

    >>> python wc.py info
    {"doc": "Hadoopy Wordcount Demo", "tasks": ["map", "reduce"]}
