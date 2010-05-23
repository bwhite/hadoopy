import subprocess
import re
import os

import hadoopy.freeze


def _script_name_from_path(script_path):
    return re.search(r'([^/]+$)', script_path).group(1)


def _find_hstreaming():
    """Finds the whole path to the hadoop streaming jar.

    If the environmental var HADOOP_HOME is specified, then start the search from there.

    Returns:
        Full path to the hadoop streaming jar if found, else return an empty string.
    """
    try:
        search_root = os.environ['HADOOP_HOME']
    except KeyError:
        search_root = '/'
    cmd = 'find %s -name hadoop*streaming.jar1' % (search_root)
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.communicate()[0].split('\n')[0]
    

def launch(in_name, out_name, script_path, mapper=True, reducer=True,
           combiner=False, partitioner=False, files=(), jobconfs=(), cmdenvs=(),
           copy_script=True, hstreaming=None, name=None, use_typedbytes=True,
           use_seqoutput=True, use_autoinput=True, pretend=False, **kw):
    """Run Hadoop given the parameters

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)
        mapper: If True, the mapper is "script.py map".
            If string, the mapper is the value
        reducer: If True (default), the reducer is "script.py reduce".
            If string, the reducer is the value
        combiner: If True, the reducer is "script.py combine" (default False).
            If string, the combiner is the value
        partitioner: If True, the partitioner is the value.
        copy_script: If True, the script is added to the files list.
        files: Extra files (other than the script) (string or list).
            NOTE: Hadoop copies the files into working directory (path errors!).
        jobconfs: Extra jobconf parameters (string or list)
        cmdenvs: Extra cmdenv parameters (string or list)
        hstreaming: The full hadoop streaming path to call.
        use_typedbytes: If True (default), use typedbytes IO.
        use_seqoutput: True (default), output sequence file. If False, output is text.
        use_autoinput: If True (default), sets the input format to auto.
        pretend: If true, only build the command and return.

    Returns:
        The hadoop command called.

    Raises:
        subprocess.CalledProcessError: Hadoop error.
        OSError: Hadoop streaming not found.
    """
    try:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    except TypeError:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    script_name = _script_name_from_path(script_path)
    if mapper == True:
        mapper = ' '.join((script_name, 'map'))
    if reducer == True:
        reducer = ' '.join((script_name, 'reduce'))
    if combiner == True:
        combiner = ' '.join((script_name, 'combine'))
    cmd = ('%s -output %s'%(hadoop_cmd, out_name)).split()
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
    # Add files
    if isinstance(files, str):
        files = [files]
    if copy_script:
        files = list(files)
        files.append(script_path)
        
    for f in files:
        cmd += ['-file', f]
    # Add jobconfs
    if isinstance(jobconfs, str):
        jobconfs = [jobconfs]
    if name == None:
        jobconfs = list(jobconfs)
        jobconfs.append('mapred.job.name=%s' % (script_name))
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
        cmd += ['-outputformat', 'org.apache.hadoop.mapred.SequenceFileOutputFormat']
    # Add InputFormat
    if use_autoinput:
        cmd += ['-inputformat', 'AutoInputFormat']
    # Run command and wait till it has completed
    if not pretend:
        print('HadooPY: Running[%s]' % (' '.join(cmd)))
        subprocess.check_call(cmd)
    return ' '.join(cmd)


def launch_frozen(in_name, out_name, script_path, **kw):
    """Freezes a script and then launches it.

    Consult hadoopy.freeze.freeze and hadoopy.launch for optional kw args.

    Args:
        in_name: Input path (string or list)
        out_name: Output path
        script_path: Path to the script (e.g., script.py)

    Returns:
        A tuple of the freeze and hadoop commands.

    Raises:
        subprocess.CalledProcessError: Hadoop or Cxfreeze error.
        OSError: Hadoop streaming or Cxfreeze not found.
    """
    freeze_cmd = hadoopy.freeze.freeze(script_path, **kw)
    # Remove extension
    if script_path.endswith('.py'):
        script_path = script_path[:-3]
    try:
        files = kw['files']
    except KeyError:
        files = []
    else:
        if isinstance(files, str):
            files = [files]
    try:
        target_dir = kw['target_dir']
    except KeyError:
        target_dir = 'frozen'
    files.append(target_dir)
    # Do not copy script
    kw['copy_script'] = False
    kw['files'] = files
    launch_cmd = launch(in_name, out_name, script_path, **kw)
    return freeze_cmd, launch_cmd
