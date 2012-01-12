#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'


import subprocess
import os
import shutil
import hadoopy._freeze
import sys
import json
import tempfile
import stat
import multiprocessing
import Queue

# These two globals are only used in the follow function
WARNED_HADOOP_HOME = False
HADOOP_STREAMING_PATH_CACHE = None


def _find_hstreaming():
    """Finds the whole path to the hadoop streaming jar.

    If the environmental var HADOOP_HOME is specified, then start the search
    from there.

    Returns:
        Full path to the hadoop streaming jar if found, else return an empty
        string.
    """
    global WARNED_HADOOP_HOME, HADOOP_STREAMING_PATH_CACHE
    if HADOOP_STREAMING_PATH_CACHE:
        return HADOOP_STREAMING_PATH_CACHE
    try:
        search_root = os.environ['HADOOP_HOME']
    except KeyError:
        search_root = '/'
        if not WARNED_HADOOP_HOME:
            print('Hadoopy: Set the HADOOP_HOME environmental variable to your hadoop path to improve performance. (e.g., Put [export HADOOP_HOME="/home/user/hadoop-0.20.2+320"] in /home/user/.bashrc)')
            WARNED_HADOOP_HOME = True
    cmd = 'find %s -name hadoop*streaming*.jar' % (search_root)
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    HADOOP_STREAMING_PATH_CACHE = p.communicate()[0].split('\n')[0]
    return HADOOP_STREAMING_PATH_CACHE


def _parse_info(script_path, python_cmd='python'):
    p = subprocess.Popen([python_cmd, script_path, 'info'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return json.loads(stdout)


def launch(in_name, out_name, script_path, partitioner=False, files=(), jobconfs=(),
           cmdenvs=(), copy_script=True, wait=True, hstreaming=None, name=None,
           use_typedbytes=True, use_seqoutput=True, use_autoinput=True,
           add_python=True, config=None, pipe=True,
           python_cmd="python", num_mappers=None, num_reducers=None,
           script_dir='', remove_ext=False, **kw):
    """Run Hadoop given the parameters

    :param in_name: Input path (string or list)
    :param out_name: Output path
    :param script_path: Path to the script (e.g., script.py)
    :param partitioner: If True, the partitioner is the value.
    :param files: Extra files (other than the script) (iterator).  NOTE: Hadoop copies the files into working directory
    :param jobconfs: Extra jobconf parameters (iterator)
    :param cmdenvs: Extra cmdenv parameters (iterator)
    :param copy_script: If True, the script is added to the files list.
    :param wait: If True, wait till the process is completed (default True) this is useful if you want to run multiple jobs concurrently by using the 'process' entry in the output.
    :param hstreaming: The full hadoop streaming path to call.
    :param name: Set the job name to this (default None, job name is the script name)
    :param use_typedbytes: If True (default), use typedbytes IO.
    :param use_seqoutput: True (default), output sequence file. If False, output is text.
    :param use_autoinput: If True (default), sets the input format to auto.
    :param add_python: If true, use 'python script_name.py'
    :param config: If a string, set the hadoop config path
    :param pipe: If true (default) then call user code through a pipe to isolate it and stop bugs when printing to stdout.  See project docs.
    :param python_cmd: The python command to use. The default is "python". Can be used to override the system default python, e.g. python_cmd = "python2.6"

    :param num_mappers: The number of mappers to use, i.e. the argument given to 'numMapTasks'. If None, then do not specify this argument to hadoop streaming.
    :param num_reducers: The number of reducers to use, i.e. the argument given to 'numReduceTasks'. If None, then do not specify this argument to hadoop streaming.
    :param script_dir: Where the script is relative to working dir, will be prefixed to script_path with a / (default '' is current dir)
    :param remove_ext: If True, remove the script extension (default False)

    :rtype: Dictionary with some of the following entries (depending on options)
    :returns: freeze_cmds: Freeze command(s) ran
    :returns: frozen_tar_path: HDFS path to frozen file
    :returns: hadoop_cmds: Hadoopy command(s) ran
    :returns: process: subprocess.Popen object
    :returns: output: Iterator of (key, value) pairs
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.
    :raises: TypeError: Input types are not correct.
    """
    if isinstance(files, str) or isinstance(jobconfs, str) or isinstance(cmdenvs, str):
        raise TypeError('files,  jobconfs, and cmdenvs must be iterators of strings and not strings!')
    out = {}
    try:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    except TypeError:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    job_name = os.path.basename(script_path).rsplit('.', 1)[0]
    script_name = os.path.basename(script_path)
    if remove_ext:
        script_name = script_name.rsplit('.', 1)[0]
    if add_python:
        script_name = '%s %s' % (python_cmd, script_name)
    if script_dir:
        script_name = ''.join([script_dir, '/', script_name])
    script_info = _parse_info(script_path, python_cmd)
    if 'map' in script_info['tasks']:
        c = 'pipe map' if pipe else 'map'
        mapper = ' '.join((script_name, c))
    if 'reduce' in script_info['tasks']:
        c = 'pipe reduce' if pipe else 'reduce'
        reducer = ' '.join((script_name, c))
    else:
        reducer = None
    if 'combine' in script_info['tasks']:
        c = 'pipe combine' if pipe else 'combine'
        combiner = ' '.join((script_name, c))
    else:
        combiner = None
    cmd = ('%s -output %s' % (hadoop_cmd, out_name)).split()
    # Add inputs
    if isinstance(in_name, str):
        in_name = [in_name]
    for f in in_name:
        cmd += ['-input', f]
    # Add mapper/reducer
    cmd += ['-mapper',
            '"%s"' % (mapper)]
    if reducer:
        cmd += ['-reducer',
                '"%s"' % (reducer)]
    else:
        cmd += ['-reducer',
                'NONE']
    if combiner:
        cmd += ['-combiner',
                '"%s"' % (combiner)]
    if partitioner:
        cmd += ['-partitioner',
                '"%s"' % (partitioner)]
    if num_mappers:
        cmd += ['-numMapTasks', "'%i'"%(int(num_mappers))]
    if num_reducers:
        cmd += ['-numReduceTasks', "'%i'"%(int(num_reducers))]
    # Add files
    if copy_script:
        files = list(files)
        files.append(script_path)
    # BUG: CDH3 doesn't copy directories properly this enumerates them
    new_files = []
    for f in files:
        if os.path.isdir(f):
            new_files += ['%s/%s' % (f, x) for x in os.listdir(f)]
        else:
            new_files.append(f)
    files = new_files
    del new_files
    # END BUG
    for f in files:
        cmd += ['-file', f]
    # Add jobconfs
    jobconfs = list(jobconfs)
    if name == None:
        jobconfs.append('mapred.job.name=%s' % (job_name))
    else:
        jobconfs.append('mapred.job.name=%s' % (str(name)))
    for jobconf in jobconfs:
        cmd += ['-jobconf', jobconf]
    # Add cmdenv
    for cmdenv in cmdenvs:
        cmd += ['-cmdenv', cmdenv]
    # Add IO
    if use_typedbytes:
        cmd += ['-io', 'typedbytes']
    # Add Outputformat
    if use_seqoutput:
        cmd += ['-outputformat',
                'org.apache.hadoop.mapred.SequenceFileOutputFormat']
    # Add InputFormat
    if use_autoinput:
        cmd += ['-inputformat', 'AutoInputFormat']
    # Add config
    if config:
        cmd += ['--config', config]
    # Run command and wait till it has completed
    print('/\\%s%s Output%s/\\' % ('-' * 10, 'Hadoop', '-' * 10))
    print('hadoopy: Running[%s]' % (' '.join(cmd)))
    process = subprocess.Popen(' '.join(cmd), shell=True)
    out['process'] = process
    if wait:
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, ' '.join(cmd))
    print('\\/%s%s Output%s\\/' % ('-' * 10, 'Hadoop', '-' * 10))
    # NOTE(brandyn): Postpones calling readtb

    def _read_out():
        for x in hadoopy.readtb(out_name):
            yield x
        out['output'] = _read_out()
    out['hadoop_cmds'] = [' '.join(cmd)]
    return out


def launch_frozen(in_name, out_name, script_path, frozen_tar_path=None,
                  temp_path='_hadoopy_temp',
                  **kw):
    """Freezes a script and then launches it.

    This function will freeze your python program, and place it on HDFS
    in 'temp_path'.  It will not remove it afterwards as they are typically
    small, you can easily reuse/debug them, and to avoid any risks involved
    with removing the file.

    :param in_name: Input path (string or list)
    :param out_name: Output path
    :param script_path: Path to the script (e.g., script.py)
    :param frozen_tar_path: If not None, use this path to a previously frozen archive.  You can get such a path from the return value of this function, it is particularly helpful in iterative programs.
    :param temp_path: HDFS path that we can use to store temporary files (default to _hadoopy_temp)
    :param partitioner: If True, the partitioner is the value.
    :param wait: If True, wait till the process is completed (default True) this is useful if you want to run multiple jobs concurrently by using the 'process' entry in the output.
    :param files: Extra files (other than the script) (iterator).  NOTE: Hadoop copies the files into working directory
    :param jobconfs: Extra jobconf parameters (iterator)
    :param cmdenvs: Extra cmdenv parameters (iterator)
    :param hstreaming: The full hadoop streaming path to call.
    :param name: Set the job name to this (default None, job name is the script name)
    :param use_typedbytes: If True (default), use typedbytes IO.
    :param use_seqoutput: True (default), output sequence file. If False, output is text.
    :param use_autoinput: If True (default), sets the input format to auto.
    :param config: If a string, set the hadoop config path
    :param pipe: If true (default) then call user code through a pipe to isolate it and stop bugs when printing to stdout.  See project docs.
    :param python_cmd: The python command to use. The default is "python". Can be used to override the system default python, e.g. python_cmd = "python2.6"

    :param num_mappers: The number of mappers to use, i.e. the argument given to 'numMapTasks'. If None, then do not specify this argument to hadoop streaming.
    :param num_reducers: The number of reducers to use, i.e. the argument given to 'numReduceTasks'. If None, then do not specify this argument to hadoop streaming.

    :rtype: Dictionary with some of the following entries (depending on options)
    :returns: freeze_cmds: Freeze command(s) ran
    :returns: frozen_tar_path: HDFS path to frozen file
    :returns: hadoop_cmds: Hadoopy command(s) ran
    :returns: process: subprocess.Popen object
    :returns: output: Iterator of (key, value) pairs
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.
    :raises: TypeError: Input types are not correct.
    """
    if (('files' in kw and isinstance(kw['files'], str)) or
        ('jobconfs' in kw and isinstance(kw['jobconfs'], str)) or
        ('cmdenvs' in kw and isinstance(kw['cmdenvs'], str))):
        raise TypeError('files,  jobconfs, and cmdenvs must be iterators of strings and not strings!')
    cmds = []
    if not frozen_tar_path:
        freeze_out = hadoopy.freeze_script(script_path, temp_path=temp_path)
        frozen_tar_path = freeze_out['frozen_tar_path']
        cmds += freeze_out['cmds']
    try:
        jobconfs = list(kw['jobconfs'])
    except KeyError:
        jobconfs = []
    jobconfs.append('"mapred.cache.archives=%s#_frozen"' % frozen_tar_path)
    jobconfs.append('"mapreduce.job.cache.archives=%s#_frozen"' % frozen_tar_path)
    kw['copy_script'] = False
    kw['add_python'] = False
    kw['jobconfs'] = jobconfs
    out = launch(in_name, out_name, script_path,
                 script_dir='_frozen', remove_ext=True, **kw)
    out['freeze_cmds'] = cmds
    out['frozen_tar_path'] = frozen_tar_path
    return out


def _local_reader(worker_queue_maxsize, q_recv, q_send, in_r_fd, in_w_fd, out_r_fd, out_w_fd):
    os.close(in_r_fd)
    os.close(in_w_fd)
    os.close(out_w_fd)
    q = Queue.Queue()
    with hadoopy.TypedBytesFile(read_fd=out_r_fd) as tbfp_r:
        for num, kv in enumerate(tbfp_r):
            while True:
                while not q_recv.poll() and not q.empty():
                    q_send.send(q.get())
                try:
                    q.put_nowait(kv)
                except Queue.Full:
                    continue
                else:
                    break
    while not q.empty():
        q_send.send(q.get())
    q_send.close()


def launch_local(in_name, out_name, script_path, max_input=-1,
                 files=(), cmdenvs=(), pipe=True, python_cmd='python', remove_tempdir=True,
                 worker_queue_maxsize=0, **kw):
    """A simple local emulation of hadoop

    This doesn't run hadoop and it doesn't support many advanced features, it
    is intended for simple debugging.  The input/output uses HDFS if an
    HDFS path is given. This allows for small tasks to be run locally
    (primarily while debugging). A temporary working directory is used and
    removed.

    Support

    * Environmental variables
    * Map-only tasks
    * Combiner
    * Files
    * Pipe (see below)
    * Display of stdout/stderr
    * Iterator of KV pairs as input or output (bypassing HDFS)

    :param in_name: Input path (string or list of strings) or Iterator of (key, value).  If it is an iterator then no input is taken from HDFS.
    :param out_name: Output path or None.  If None then output is not placed on HDFS, it is available through the 'output' key of the return value.
    :param script_path: Path to the script (e.g., script.py)
    :param max_input: Maximum number of Mapper inputs, if < 0 (default) then unlimited.
    :param files: Extra files (other than the script) (iterator).  NOTE: Hadoop copies the files into working directory
    :param cmdenvs: Extra cmdenv parameters (iterator)
    :param pipe: If true (default) then call user code through a pipe to isolate it and stop bugs when printing to stdout.  See project docs.
    :param python_cmd: The python command to use. The default is "python".  Can be used to override the system default python, e.g. python_cmd = "python2.6"
    :param remove_tempdir: If True (default), then rmtree the temporary dir, else print its location.  Useful if you need to see temporary files or how input files are copied.
    :param worker_queue_maxsize: The number of elements the queue holding results from the worker task will hold (default 0 which is unlimited).
    :rtype: Dictionary with some of the following entries (depending on options)
    :returns: freeze_cmds: Freeze command(s) ran
    :returns: frozen_tar_path: HDFS path to frozen file
    :returns: hadoop_cmds: Hadoopy command(s) ran
    :returns: process: subprocess.Popen object
    :returns: output: Iterator of (key, value) pairs
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.
    :raises: TypeError: Input types are not correct.
    """
    if isinstance(files, str) or isinstance(cmdenvs, str) or ('cmdenvs' in kw and isinstance(kw['cmdenvs'], str)):
        raise TypeError('files,  jobconfs, and cmdenvs must be iterators of strings and not strings!')
    print('Local[%s]' % script_path)
    script_info = _parse_info(script_path, python_cmd)
    if not files:
        files = []
    files.append(script_path)
    files = [os.path.abspath(f) for f in files]
    # Setup env
    env = dict(os.environ)
    env['stream_map_input'] = 'typedbytes'
    if cmdenvs:
        for cmdenv in cmdenvs:
            k, v = cmdenv.split('=', 1)
            env[k] = v

    def _run_task(task, kvs, env):
        sys.stdout.flush()
        cur_max_input = max_input if task == 'map' else -1
        if pipe:
            task = 'pipe %s' % task
        in_r_fd, in_w_fd = os.pipe()
        out_r_fd, out_w_fd = os.pipe()
        cmd = ('%s %s %s' % (python_cmd, script_path, task)).split()
        a = os.fdopen(in_r_fd, 'r')
        b = os.fdopen(out_w_fd, 'w')
        q_recv, q_send = multiprocessing.Pipe(True)
        reader_process = multiprocessing.Process(target=_local_reader, args=(worker_queue_maxsize, q_recv, q_send, in_r_fd, in_w_fd, out_r_fd, out_w_fd))
        reader_process.start()
        os.close(out_r_fd)
        q_send.close()
        try:
            p = subprocess.Popen(cmd,
                                 stdin=a,
                                 stdout=b,
                                 close_fds=True,
                                 env=env)
            a.close()
            b.close()
            # main =(in_fd)> p
            # main <(out_fd)= p
            with hadoopy.TypedBytesFile(write_fd=in_w_fd, flush_writes=True) as tbfp_w:
                for num, kv in enumerate(kvs):
                    if cur_max_input >= 0 and cur_max_input <= num:
                        break
                    if reader_process.exitcode:
                        raise EOFError('Reader process died[%s]' % reader_process.exitcode)
                    while q_recv.poll():
                        try:
                            yield q_recv.recv()
                        except EOFError:
                            break
                    tbfp_w.write(kv)
            # Get any remaining values
            while True:
                if reader_process.exitcode:
                    raise EOFError('Reader process died[%s]' % reader_process.exitcode)
                try:
                    yield q_recv.recv()
                except EOFError:
                    break
            print('********  Done with inputs and cleaned up')
        finally:
            print('Closing up shop')
            q_recv.close()
            reader_process.join()
            p.kill()
            p.wait()

    orig_pwd = os.path.abspath('.')
    new_pwd = tempfile.mkdtemp()
    out = {}
    try:
        print('Hadoopy: Launch local changing current directory to [%s]' % new_pwd)
        os.chdir(new_pwd)
        if files:
            for f in files:
                shutil.copy(f, os.path.basename(f))
        script_path = os.path.basename(script_path)
        os.chmod(script_path, stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR)
        if isinstance(in_name, str) or (in_name and isinstance(in_name, (list, tuple)) and isinstance(in_name[0], str)):
            in_kvs = hadoopy.readtb(in_name)
        else:
            in_kvs = in_name
        if 'reduce' in script_info['tasks']:
            kvs = list(_run_task('map', in_kvs, env))
            if 'combine' in script_info['tasks']:
                print('COMBINER ----------------------------')
                kvs = hadoopy.Test.sort_kv(kvs)
                kvs = list(_run_task('combine', kvs, env))
            print('REDUCER ----------------------------')
            kvs = hadoopy.Test.sort_kv(kvs)
            kvs = _run_task('reduce', kvs, env)
        else:
            print('********** MapOnly Task')
            kvs = _run_task('map', in_kvs, env)
    except OSError, e:
        print('Error: Ensure that [%s] starts with "#!/usr/bin/env python".' % script_path)
        raise e
    else:
        if out_name is not None:
            print('Start writing')
            hadoopy.writetb(out_name, kvs)
            print('Done writing')
            out['output'] = hadoopy.readtb(out_name)
        else:
            out['output'] = iter(list(kvs))  # TODO(brandyn): Potential problem if using large values, fixes changing dir early
        return out
    finally:
        os.chdir(orig_pwd)
        if remove_tempdir:
            shutil.rmtree(new_pwd)
        else:
            print('Temporary directory not removed[%s]' % new_pwd)
