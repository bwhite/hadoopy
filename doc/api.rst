API
===

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
