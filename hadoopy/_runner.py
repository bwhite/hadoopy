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
import json
import tempfile
import stat
import logging
import time
import select
import atexit

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
    cmd = 'find %s -name hadoop*streaming*.jar' % (search_root)
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    HADOOP_STREAMING_PATH_CACHE = p.communicate()[0].split('\n')[0]
    if search_root == '/' and not WARNED_HADOOP_HOME:
        WARNED_HADOOP_HOME = True
        hadoop_home = HADOOP_STREAMING_PATH_CACHE[:HADOOP_STREAMING_PATH_CACHE.rfind('/contrib/')]
        logging.warn('Set the HADOOP_HOME environmental variable to your hadoop path to improve performance. Put the following [export HADOOP_HOME="%s"] in ~/.bashrc' % hadoop_home)
    return HADOOP_STREAMING_PATH_CACHE


def _listeq_to_dict(jobconfs):
    """Convert iterators of 'key=val' into a dictionary with later values taking priority."""
    if not isinstance(jobconfs, dict):
        jobconfs = dict(x.split('=', 1) for x in jobconfs)
    return dict((str(k), str(v)) for k, v in jobconfs.items())


def _parse_info(script_path, python_cmd='python'):
    if not os.path.exists(script_path):
        raise ValueError('Script [%s] not found!' % script_path)
    p = subprocess.Popen([python_cmd, script_path, 'info'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    try:
        info = json.loads(stdout)
    except ValueError:
        raise ValueError('Cannot execute script [%s]!  Script output below...\nstdout\n%s\nstderr\n%s' % (script_path, stdout, stderr))
    info['jobconfs'] = _listeq_to_dict(info.get('jobconfs', ()))
    return info


def _check_requirements(files, cmdenvs, required_files, required_cmdenvs):
    files = set(os.path.basename(x) for x in files)
    cmdenvs = set(cmdenvs)
    required_files = set(required_files)
    required_cmdenvs = set(required_cmdenvs)
    missing_files = required_files - files
    missing_cmdenvs = required_cmdenvs - cmdenvs
    if missing_files or missing_cmdenvs:
        error_out = []
        if missing_files:
            error_out.append('Missing required file(s), include them using the "files" argument: [%s]' % ', '.join(missing_files))
        if missing_cmdenvs:
            error_out.append('Missing required cmdenvs(s), include them using the "cmdenvs" argument: [%s]' % ', '.join(missing_cmdenvs))
        raise ValueError('\n'.join(error_out))


def _check_script(script_path, files, python_cmd):
    logging.info('Sanity checking script in a temp directory... (disable with check_script=False)')
    orig_pwd = os.path.abspath('.')
    try:
        temp_dir = tempfile.mkdtemp()
        for f in files:
            if f.endswith('.py') or f.endswith('.pyc'):
                shutil.copy(f, temp_dir)
        os.chdir(temp_dir)
        try:
            _parse_info(os.path.basename(script_path), python_cmd)
        except ValueError:
            logging.error('Sanity check failed: This is often due to local imports not included in the "files" argument.')
            raise
    finally:
        shutil.rmtree(temp_dir)
        os.chdir(orig_pwd)
    

def launch(in_name, out_name, script_path, partitioner=False, files=(), jobconfs=(),
           cmdenvs=(), libjars=(), input_format=None, output_format=None, copy_script=True,
           wait=True, hstreaming=None, name=None,
           use_typedbytes=True, use_seqoutput=True, use_autoinput=True,
           remove_output=False, add_python=True, config=None, pipe=True,
           python_cmd="python", num_mappers=None, num_reducers=None,
           script_dir='', remove_ext=False, check_script=True, make_executable=True,
           required_files=(), required_cmdenvs=(), **kw):
    """Run Hadoop given the parameters

    :param in_name: Input path (string or list)
    :param out_name: Output path
    :param script_path: Path to the script (e.g., script.py)
    :param partitioner: If True, the partitioner is the value.
    :param files: Extra files (other than the script) (iterator).  NOTE: Hadoop copies the files into working directory
    :param jobconfs: Extra jobconf parameters (iterator of strings or dict)
    :param cmdenvs: Extra cmdenv parameters (iterator of strings or dict)
    :param libjars: Extra jars to include with the job (iterator of strings).
    :param input_format: Custom input format (if set overrides use_autoinput)
    :param output_format: Custom output format (if set overrides use_seqoutput)
    :param copy_script: If True, the script is added to the files list.
    :param wait: If True, wait till the process is completed (default True) this is useful if you want to run multiple jobs concurrently by using the 'process' entry in the output.
    :param hstreaming: The full hadoop streaming path to call.
    :param name: Set the job name to this (default None, job name is the script name)
    :param use_typedbytes: If True (default), use typedbytes IO.
    :param use_seqoutput: True (default), output sequence file. If False, output is text.  If output_format is set, this is not used.
    :param use_autoinput: If True (default), sets the input format to auto.  If input_format is set, this is not used.
    :param remove_output: If True, output directory is removed if it exists. (defaults to False)
    :param add_python: If true, use 'python script_name.py'
    :param config: If a string, set the hadoop config path
    :param pipe: If true (default) then call user code through a pipe to isolate it and stop bugs when printing to stdout.  See project docs.
    :param python_cmd: The python command to use. The default is "python". Can be used to override the system default python, e.g. python_cmd = "python2.6"

    :param num_mappers: The number of mappers to use (i.e., jobconf mapred.map.tasks=num_mappers).
    :param num_reducers: The number of reducers to use (i.e., jobconf mapred.reduce.tasks=num_reducers).
    :param script_dir: Where the script is relative to working dir, will be prefixed to script_path with a / (default '' is current dir)
    :param remove_ext: If True, remove the script extension (default False)
    :param check_script: If True, then copy script and .py(c) files to a temporary directory and verify that it can be executed.  This catches the majority of errors related to not included locally imported files. (default True)
    :param make_executable: If True, ensure that script is executable and has a #! line at the top.
    :param required_files: Iterator of files that must be specified (verified against the files argument)
    :param required_cmdenvs: Iterator of cmdenvs that must be specified (verified against the cmdenvs argument)

    :rtype: Dictionary with some of the following entries (depending on options)
    :returns: freeze_cmds: Freeze command(s) ran
    :returns: frozen_tar_path: HDFS path to frozen file
    :returns: hadoop_cmds: Hadoopy command(s) ran
    :returns: process: subprocess.Popen object
    :returns: output: Iterator of (key, value) pairs
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.
    :raises: TypeError: Input types are not correct.
    :raises: ValueError: Script not found or check_script failed
    """
    if isinstance(files, (str, unicode)) or isinstance(jobconfs, (str, unicode)) or isinstance(cmdenvs, (str, unicode)):
        raise TypeError('files,  jobconfs, and cmdenvs must be iterators of strings and not strings!')
    jobconfs = _listeq_to_dict(jobconfs)
    cmdenvs = _listeq_to_dict(cmdenvs)
    libjars = list(libjars)
    if make_executable and script_path.endswith('.py') and pipe:
        script_path = hadoopy._runner._make_script_executable(script_path)
    script_info = _parse_info(script_path, python_cmd)
    job_name = os.path.basename(script_path).rsplit('.', 1)[0]
    script_name = os.path.basename(script_path)
    # Add required cmdenvs/files, num_reducers from script
    required_cmdenvs = list(script_info.get('required_cmdenvs', ())) + list(required_cmdenvs)
    required_files = list(script_info.get('required_files', ())) + list(required_files)
    _check_requirements(files, cmdenvs, required_files, required_cmdenvs)
    try:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    except TypeError:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    if remove_ext:
        script_name = script_name.rsplit('.', 1)[0]
    if add_python:
        script_name = '%s %s' % (python_cmd, script_name)
    if script_dir:
        script_name = ''.join([script_dir, '/', script_name])
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
    cmd = [hadoop_cmd]
    # Add libjars
    if libjars:
        cmd += ['--libjars', ','.join(libjars)]
    cmd += ['-output', out_name]
    # Add inputs
    if isinstance(in_name, (str, unicode)):
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
    if num_mappers is not None:
        if 'mapred.map.tasks' not in jobconfs:
            jobconfs['mapred.map.tasks'] = str(num_mappers)
    if num_reducers is not None:
        if 'mapred.reduce.tasks' not in jobconfs:
            jobconfs['mapred.reduce.tasks'] = str(num_reducers)
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
    if check_script:
        _check_script(script_path, files, python_cmd)
    for f in files:
        cmd += ['-file', f]
    # Add jobconfs
    if name == None:
        jobconfs['mapred.job.name'] = job_name
    else:
        jobconfs['mapred.job.name'] = str(name)
    # Handle additional jobconfs listed in the job itself
    # these go at the beginning of the list as later jobconfs
    # override them.  Launch specified confs override job specified ones
    # as Hadoop takes the last one you provide.
    jobconfs_all = dict(script_info['jobconfs'])
    jobconfs_all.update(jobconfs)
    jobconfs = jobconfs_all
    for x in jobconfs_all.items():
        cmd += ['-jobconf', '"%s=%s"' % x]
    # Add cmdenv
    for x in cmdenvs.items():
        cmd += ['-cmdenv', '"%s=%s"' % x]
    # Add IO
    if use_typedbytes:
        cmd += ['-io', 'typedbytes']
    # Add Outputformat
    if output_format is not None:
        cmd += ['-outputformat', output_format]
    else:
        if use_seqoutput:
            cmd += ['-outputformat',
                    'org.apache.hadoop.mapred.SequenceFileOutputFormat']
    # Add InputFormat
    if input_format is not None:
        cmd += ['-inputformat', input_format]
    else:
        if use_autoinput:
            cmd += ['-inputformat', 'AutoInputFormat']
    # Add config
    if config:
        cmd += ['--config', config]
    # Remove output
    if remove_output and hadoopy.exists(out_name):
        logging.warn('Removing output directory [%s]' % out_name)
        hadoopy.rmr(out_name)
    # Run command and wait till it has completed
    hadoop_start_time = time.time()
    logging.info('/\\%s%s Output%s/\\' % ('-' * 10, 'Hadoop', '-' * 10))
    logging.info('hadoopy: Running[%s]' % (' '.join(cmd)))

    out = {}
    out['process'] = process = subprocess.Popen(' '.join(cmd), shell=True,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE)
    # NOTE(brandyn): Read a line from stdout/stderr and log it.  This allows us
    # to use the logging system instead printing to the console.
    if wait:
        def check_fps():
            fps = select.select([process.stdout, process.stderr], [], [], 1.)[0]
            for fp in fps:
                line = fp.readline()
                if line:  # NOTE(brandyn): Should have at least a newline
                    line = line[:-1]
                    if line.find(' ERROR ') != -1:
                        logging.error(line)
                    elif line.find(' WARN ') != -1:
                        logging.warn(line)
                    else:
                        logging.info(line)
        while process.poll() is None:
            check_fps()
        check_fps()
        if process.wait():
            raise subprocess.CalledProcessError(process.returncode, ' '.join(cmd))
        logging.info('Hadoop took [%f] seconds' % (time.time() - hadoop_start_time))
    logging.info('\\/%s%s Output%s\\/' % ('-' * 10, 'Hadoop', '-' * 10))
    # NOTE(brandyn): Postpones calling readtb

    def _read_out():
        for x in hadoopy.readtb(out_name):
            yield x
        out['output'] = _read_out()
    out['hadoop_cmds'] = [' '.join(cmd)]
    return out


def launch_frozen(in_name, out_name, script_path, frozen_tar_path=None,
                  temp_path='_hadoopy_temp', cache=True, check_script=False,
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
    :param cache: If True (default) then use previously frozen scripts.  Cache is stored in memory (not persistent).
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
    :param check_script: If True, then copy script and .py(c) files to a temporary directory and verify that it can be executed.  This catches the majority of errors related to not included locally imported files. The default is False when using launch_frozen as the freeze process packages local files.

    :rtype: Dictionary with some of the following entries (depending on options)
    :returns: freeze_cmds: Freeze command(s) ran
    :returns: frozen_tar_path: HDFS path to frozen file
    :returns: hadoop_cmds: Hadoopy command(s) ran
    :returns: process: subprocess.Popen object
    :returns: output: Iterator of (key, value) pairs
    :raises: subprocess.CalledProcessError: Hadoop error.
    :raises: OSError: Hadoop streaming not found.
    :raises: TypeError: Input types are not correct.
    :raises: ValueError: Script not found
    """
    if (('files' in kw and isinstance(kw['files'], (str, unicode))) or
        ('jobconfs' in kw and isinstance(kw['jobconfs'], (str, unicode))) or
        ('cmdenvs' in kw and isinstance(kw['cmdenvs'], (str, unicode)))):
        raise TypeError('files,  jobconfs, and cmdenvs must be iterators of strings and not strings!')
    if 'jobconfs' in kw:
        kw['jobconfs'] = _listeq_to_dict(kw['jobconfs'])
    if 'cmdenvs' in kw:
        kw['cmdenvs'] = _listeq_to_dict(kw['cmdenvs'])
    cmds = []
    if not frozen_tar_path:
        freeze_out = hadoopy.freeze_script(script_path, temp_path=temp_path, cache=cache)
        frozen_tar_path = freeze_out['frozen_tar_path']
        cmds += freeze_out['cmds']
    jobconfs = kw.get('jobconfs', {})
    jobconfs['mapred.cache.archives'] = '%s#_frozen' % frozen_tar_path
    jobconfs['mapreduce.job.cache.archives'] = '%s#_frozen' % frozen_tar_path
    kw['copy_script'] = False
    kw['add_python'] = False
    kw['jobconfs'] = jobconfs
    out = launch(in_name, out_name, script_path,
                 script_dir='_frozen', remove_ext=True, check_script=check_script,
                 make_executable=False, **kw)
    out['freeze_cmds'] = cmds
    out['frozen_tar_path'] = frozen_tar_path
    return out


def _make_script_executable(script_path, temp_copy=True):
    cur_mode = os.stat(script_path).st_mode & 07777
    script_data = open(script_path).read()
    if not stat.S_IXUSR & cur_mode or script_data[:2] != '#!':
        if temp_copy:
            logging.warn('Script is not executable which is a requirement when pipe=True.  A temporary copy will be modified to correct this.')
            temp_fp = tempfile.NamedTemporaryFile(suffix=os.path.basename(script_path))
            shutil.copy(script_path, temp_fp.name)
            atexit.register(temp_fp.close)  # NOTE(brandyn): This keeps the file from being deleted when wait=False in launch
            script_path = temp_fp.name
        if not stat.S_IXUSR & cur_mode:
            logging.warn('Making script [%s] executable.' % script_path)
            os.chmod(script_path, stat.S_IXUSR | cur_mode)
        if script_data[:2] != '#!':
            logging.warn('Adding "#!/usr/bin/env python" to script [%s].  This will make line numbers off by one from the original.' % script_path)
            with open(script_path, 'w') as fp:
                fp.write('#!/usr/bin/env python\n' + script_data)
    return script_path
