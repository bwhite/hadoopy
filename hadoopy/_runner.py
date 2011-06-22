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
import time
import tempfile
import shutil
import hadoopy._freeze
import select


def _find_hstreaming():
    """Finds the whole path to the hadoop streaming jar.

    If the environmental var HADOOP_HOME is specified, then start the search
    from there.

    Returns:
        Full path to the hadoop streaming jar if found, else return an empty
        string.
    """
    try:
        search_root = os.environ['HADOOP_HOME']
    except KeyError:
        search_root = '/'
    cmd = 'find %s -name hadoop*streaming*.jar' % (search_root)
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    return p.communicate()[0].split('\n')[0]


def launch(in_name, out_name, script_path, mapper=True, reducer=True,
           combiner=False, partitioner=False, files=(), jobconfs=(),
           cmdenvs=(), copy_script=True, wait=True, hstreaming=None, name=None,
           use_typedbytes=True, use_seqoutput=True, use_autoinput=True,
           pretend=False, add_python=True, config=None, pipe=True,
           python_cmd="python", num_mappers=None, num_reducers=None,
           script_dir='', **kw):
    """Run Hadoop given the parameters

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)
        mapper: If True, the mapper is "script.py map".
            If string, the mapper is the value
        reducer: If True (default), the reducer is "script.py reduce".
            If string, the reducer is the value
        combiner: If True, the combiner is "script.py combine" (default False).
            If string, the combiner is the value
        partitioner: If True, the partitioner is the value.
        copy_script: If True, the script is added to the files list.
        wait: If True, wait till the process is completed (default True)
            this is useful if you want to run multiple jobs concurrently
            by using the 'process' entry in the output.
        files: Extra files (other than the script) (string or list).
            NOTE: Hadoop copies the files into working directory
        jobconfs: Extra jobconf parameters (string or list)
        cmdenvs: Extra cmdenv parameters (string or list)
        hstreaming: The full hadoop streaming path to call.
        use_typedbytes: If True (default), use typedbytes IO.
        use_seqoutput: True (default), output sequence file. If False, output
            is text.
        use_autoinput: If True (default), sets the input format to auto.
        pretend: If true, only build the command and return.
        add_python: If true, use 'python script_name.py'
        config: If a string, set the hadoop config path
        pipe: If true (default) then call user code through a pipe to isolate
            it and stop bugs when printing to stdout.  See project docs.
        python_cmd: The python command to use. The default is "python".
            Can be used to override the system default python, e.g.
            python_cmd = "python2.6"
        num_mappers: The number of mappers to use, i.e. the
            argument given to 'numMapTasks'. If None, then
            do not specify this argument to hadoop streaming.
        num_reducers: The number of reducers to use, i.e. the
            argument given to 'numReduceTasks'. If None, then
            do not specify this argument to hadoop streaming.
        script_dir: Where the script is relative to working dir, will be
            prefixed to script_path with a / (default '' is current dir)

    Returns:
        Dictionary some of the following entries (depending on options) of
        freeze_cmds: Freeze command(s) ran
        frozen_tar_path: HDFS path to frozen file
        hadoop_cmds: Hadoopy command(s) ran
        process: subprocess.Popen object
        output: Iterator of (key, value) pairs

    Raises:
        subprocess.CalledProcessError: Hadoop error.
        OSError: Hadoop streaming not found.
    """
    out = {}
    try:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    except TypeError:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    job_name = os.path.basename(script_path)
    if add_python:
        script_name = '%s %s' % (python_cmd, os.path.basename(script_path))
    else:
        script_name = os.path.basename(script_path)
    if script_dir:
        script_name = ''.join([script_dir, '/', script_name])
    if mapper == True:
        c = 'pipe map' if pipe else 'map'
        mapper = ' '.join((script_name, c))
    if reducer == True:
        c = 'pipe reduce' if pipe else 'reduce'
        reducer = ' '.join((script_name, c))
    if combiner == True:
        c = 'pipe combine' if pipe else 'combine'
        combiner = ' '.join((script_name, c))
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
    if isinstance(files, str):
        files = [files]
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
    if isinstance(jobconfs, str):
        jobconfs = [jobconfs]
    if name == None:
        jobconfs = list(jobconfs)
        jobconfs.append('mapred.job.name=%s' % (job_name))
    else:
        jobconfs.append('mapred.job.name=%s' % (str(name)))
    for jobconf in jobconfs:
        cmd += ['-jobconf', jobconf]
    # Add cmdenv
    if isinstance(cmdenvs, str):
        cmdenvs = [cmdenvs]
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
    if not pretend:
        print('/\\%s%s Output%s/\\' % ('-' * 10, 'Hadoop', '-' * 10))
        print('hadoopy: Running[%s]' % (' '.join(cmd)))
        process = subprocess.Popen(' '.join(cmd), shell=True)
        out['process'] = process
        if wait:
            if process.wait():
                raise subprocess.CalledProcessError(process.returncode, ' '.join(cmd))
        print('\\/%s%s Output%s\\/' % ('-' * 10, 'Hadoop', '-' * 10))
        out['output'] = hadoopy.readtb(out_name)
    out['hadoop_cmds'] = ' '.join(cmd)
    return out


def launch_frozen(in_name, out_name, script_path, frozen_tar_path=None,
                  temp_path='_hadoopy_temp',
                  **kw):
    """Freezes a script and then launches it.

    This function will freeze your python program, and place it on HDFS
    in 'temp_path'.  It will not remove it afterwards as they are typically
    small, you can easily reuse/debug them, and to avoid any risks involved
    with removing the file.

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)
        frozen_tar_path: If not None, use this path to a previously frozen
            archive.  You can get such a path from the return value of this
            function, it is particularly helpful in iterative programs.
        temp_path: HDFS path that we can use to store temporary files
            (default to _hadoopy_temp)
        mapper: If True, the mapper is "script.py map".
            If string, the mapper is the value
        reducer: If True (default), the reducer is "script.py reduce".
            If string, the reducer is the value
        combiner: If True, the combiner is "script.py combine" (default False).
            If string, the combiner is the value
        partitioner: If True, the partitioner is the value.
        copy_script: If True, the script is added to the files list.
        wait: If True, wait till the process is completed (default True)
            this is useful if you want to run multiple jobs concurrently
            by using the 'process' entry in the output.
        files: Extra files (other than the script) (string or list).
            NOTE: Hadoop copies the files into working directory
        jobconfs: Extra jobconf parameters (string or list)
        cmdenvs: Extra cmdenv parameters (string or list)
        hstreaming: The full hadoop streaming path to call.
        use_typedbytes: If True (default), use typedbytes IO.
        use_seqoutput: True (default), output sequence file. If False, output
            is text.
        use_autoinput: If True (default), sets the input format to auto.
        pretend: If true, only build the command and return.
        add_python: If true, use 'python script_name.py'
        config: If a string, set the hadoop config path
        pipe: If true (default) then call user code through a pipe to isolate
            it and stop bugs when printing to stdout.  See project docs.
        python_cmd: The python command to use. The default is "python".
            Can be used to override the system default python, e.g.
            python_cmd = "python2.6"
        num_mappers: The number of mappers to use, i.e. the
            argument given to 'numMapTasks'. If None, then
            do not specify this argument to hadoop streaming.
        num_reducers: The number of reducers to use, i.e. the
            argument given to 'numReduceTasks'. If None, then
            do not specify this argument to hadoop streaming.
        script_dir: Where the script is relative to working dir, will be
            prefixed to script_path with a / (default '' is current dir)

    Returns:
        Dictionary some of the following entries (depending on options) of
        freeze_cmds: Freeze command(s) ran
        frozen_tar_path: HDFS path to frozen file
        hadoop_cmds: Hadoopy command(s) ran
        process: subprocess.Popen object
        output: Iterator of (key, value) pairs

    Raises:
        subprocess.CalledProcessError: Hadoop or Cxfreeze error.
        OSError: Hadoop streaming or Cxfreeze not found.
    """
    if not frozen_tar_path:
        frozen_tar_path = temp_path + '/%f/_frozen.tar' % time.time()
        freeze_fp = tempfile.NamedTemporaryFile(suffix='.tar')
        cmds = hadoopy._freeze.freeze_to_tar(os.path.abspath(script_path), freeze_fp.name)
        hadoopy.put(freeze_fp.name, frozen_tar_path)
    if script_path.endswith('.py'):
        script_path = script_path[:-3]
    try:
        jobconfs = kw['jobconfs']
    except KeyError:
        jobconfs = []
    else:
        if isinstance(jobconfs, str):
            jobconfs = [jobconfs]
    jobconfs.append('"mapred.cache.archives=%s#_frozen"' % frozen_tar_path)
    jobconfs.append('"mapreduce.job.cache.archives=%s#_frozen"' % frozen_tar_path)
    kw['copy_script'] = False
    kw['add_python'] = False
    kw['jobconfs'] = jobconfs
    out = launch(in_name, out_name, script_path,
                 script_dir='_frozen', **kw)
    out['freeze_cmds'] = cmds
    out['frozen_tar_path'] = frozen_tar_path
    return out


def launch_local(in_name, out_name, script_path, max_input=-1, mapper=True, reducer=True,
           combiner=False, files=(), cmdenvs=(), pipe=True, **kw):
    """A simple local emulation of hadoop

    This doesn't run hadoop and it doesn't support many advanced features, it
    is intended for simple debugging.  The input/output is read from HDFS and
    they must be TypedBytes in SequenceFiles.  The output is stored on HDFS.
    This allows for small tasks to be run locally (primarily while debugging).

    Support
    - Environmental variables
    - Map-only tasks
    - Combiner
    - Files
    - Pipe (see below)
    - Display of stdout/stderr

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)
        max_input: Maximum number of Mapper inputs, if < 0 (default) then
            unlimited.
        mapper: If True, the mapper is "script.py map".
            If string, the mapper is the value
        reducer: If True (default), the reducer is "script.py reduce".
            If string, the reducer is the value
        combiner: If True, the combiner is "script.py combine" (default False).
            If string, the combiner is the value
        files: Extra files (other than the script) (string or list).
            NOTE: Hadoop copies the files into working directory
        cmdenvs: Extra cmdenv parameters (string or list)
        pipe: If true (default) then call user code through a pipe to isolate
            it and stop bugs when printing to stdout.  See project docs.
    Returns:
        Dictionary some of the following entries (depending on options) of
        freeze_cmds: Freeze command(s) ran
        frozen_tar_path: HDFS path to frozen file
        hadoop_cmds: Hadoopy command(s) ran
        process: subprocess.Popen object
        output: Iterator of (key, value) pairs

    """
    print('Hadoopy Local: In[%s] Out[%s] Script[%s]' % (in_name, out_name, script_path))
    # Copy files
    if files:
        if isinstance(files, str):
            files = [files]
        for f in files:
            shutil.copy(f, os.path.basename(f))

    # Setup env
    env = dict(os.environ)
    env['stream_map_input'] = 'typedbytes'
    if cmdenvs:
        if isinstance(cmdenvs, str):
            cmdenvs = [cmdenvs]
        for cmdenv in cmdenvs:
            k, v = cmdenv.split('=', 1)
            env[k] = v

    def _run_task(task, kvs, env):
        print('Running [%s]' % task)
        if pipe:
            task = 'pipe %s' % task
        in_r_fd, in_w_fd = os.pipe()
        out_r_fd, out_w_fd = os.pipe()
        cmd = ('./%s %s' % (script_path, task)).split()
        a = os.fdopen(in_r_fd, 'r')
        b = os.fdopen(out_w_fd, 'w')
        p = subprocess.Popen(cmd,
                             stdin=a,
                             stdout=b,
                             close_fds=True,
                             env=env)
        a.close()
        b.close()
        with hadoopy.TypedBytesFile(read_fd=out_r_fd) as tbfp_r:
            with hadoopy.TypedBytesFile(write_fd=in_w_fd) as tbfp_w:
                for num, kv in enumerate(kvs):
                    if max_input >= 0 and max_input <= num:
                        break
                    # Use select to see if the buffer has anything in it
                    # this minimizes the chance that the pipe will block
                    # TODO(brandyn): This is currently broken, fix it!
                    #while select.select([out_r_fd], [], [], 0)[0]:
                    #    yield tbfp_r.next()
                    tbfp_w.write(kv)
            # Get any remaining values
            for kv in tbfp_r:
                yield kv
        p.wait()
    try:
        kvs = list(_run_task('map', hadoopy.readtb(in_name), env))
        if not reducer:
            hadoopy.writetb(out_name, kvs)
            return
        if combiner:
            kvs = hadoopy.Test.sort_kv(kvs)
            kvs = list(_run_task('combine', kvs, env))
        kvs = hadoopy.Test.sort_kv(kvs)
        kvs = _run_task('reduce', kvs, env)
        hadoopy.writetb(out_name, kvs)
    except OSError, e:
        print('Error: Ensure that [%s] is executable and starts with "#!/usr/bin/env python".' % script_path)
        raise e
