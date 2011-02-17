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

import sys

import subprocess
import os
import time
import tempfile
import hadoopy._freeze

import pdb

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
        # cloudera default install
        if os.path.isdir('/usr/lib/hadoop'):
            sys.stderr.write('HADOOP_HOME not set, found /usr/lib/hadoop, searching for streaming jar\n')
            search_root = '/usr/lib/hadoop'
        else:
            sys.stderr.write('HADOOP_HOME not set, falling back to /, please set HADOOP_HOME\n')
            search_root = '/'
    cmd = "find -L %s -name 'hadoop*streaming*.jar'" % (search_root)
    p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    streamingjar = p.communicate()[0].split('\n')[0]
    if not os.path.isfile(streamingjar):
        raise Exception('cannot find streaming jar')
    else:
        return streamingjar


def launch(in_name, out_name, script_path, mapper=True, reducer=True,
           combiner=False, partitioner=False, files=(), jobconfs=(),
           cmdenvs=(), copy_script=True, hstreaming=None, name=None,
           use_typedbytes=False, use_seqoutput=False, use_autoinput=True,
           pretend=False, add_python=False, config=None, 
           python_cmd=None, num_mappers=None, num_reducers=None, 
           script_dir='',**kw):
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
        The hadoop command called.

    Raises:
        subprocess.CalledProcessError: Hadoop error.
        OSError: Hadoop streaming not found.
    """
    if hstreaming:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    else:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    job_name = os.path.basename(script_path)
    if add_python:
        if not python_cmd:
            python_cmd = sys.executable
        script_name = '%s %s' % (python_cmd, os.path.basename(script_path))
    else:
        script_name = os.path.basename(script_path)
    if script_dir:
        script_name = ''.join([script_dir, '/', script_name])
    if mapper == True:
        mapper = ' '.join((script_name, 'map'))
    if reducer == True:
        reducer = ' '.join((script_name, 'reduce'))
    if combiner == True:
        combiner = ' '.join((script_name, 'combine'))
    cmd = ('%s -output %s' % (hadoop_cmd, out_name)).split()
    # Add inputs
    if isinstance(in_name, str):
        in_name = [in_name]
    for f in in_name:
        cmd += ['-input', "'%s'" % (f,)]
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
        cmd += ['-numMapTasks', '%i' % (int(num_mappers),)]
    if num_reducers:
        cmd += ['-numReduceTasks', '%i' %(int(num_reducers),)]
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
        cmd += ['-file', "'%s'" % (f,)]
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
        subprocess.check_call(' '.join(cmd),shell=True)
        print('\\/%s%s Output%s\\/' % ('-' * 10, 'Hadoop', '-' * 10))
    return ' '.join(cmd)


def launch_frozen(in_name, out_name, script_path, temp_path='_hadoopy_temp',
                  **kw):
    """Freezes a script and then launches it.

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)
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
        verbose: If true, output to stdout all command results.

    Returns:
        The hadoop command called.

    Raises:
        subprocess.CalledProcessError: Hadoop or Cxfreeze error.
        OSError: Hadoop streaming or Cxfreeze not found.
    """
    frozen_tar_path = temp_path + '/%f/_frozen.tar.gz' % time.time()
    freeze_fp = tempfile.NamedTemporaryFile(suffix='.tar.gz')
    hadoopy._freeze.freeze_to_tar(os.path.abspath(script_path), freeze_fp.name)
    # TODO: Put this file on hdfs
    subprocess.call(('hadoop fs -put %s %s' % (freeze_fp.name,
                                               frozen_tar_path)).split())
    #hadoopy.put(freeze_fp.name, frozen_tar_path)
    # Remove extension
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
    launch_cmd = launch(in_name, out_name, script_path,
                        script_dir='_frozen', **kw)
    return launch_cmd
