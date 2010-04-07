import subprocess
import re


def _script_name_from_path(script_path):
    return re.search(r'([^/]+$)', script_path).group(1)


def _find_hstreaming():
    p = subprocess.Popen('find / -name hadoop*streaming.jar'.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.communicate()[0].split('\n')[0]


def run_hadoop(in_name, out_name, script_path, mapper=True, reducer=True,
               combiner=False, files=(), jobconfs=(), cmdenvs=(),
               compress_output=False, copy_script=True, hstreaming=None,
               name=None, frozen_path=None, use_typedbytes=True,
               use_seqoutput=True, use_autoinput=True, pretend=False):
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
        copy_script: If True, the script is added to the files list.
        files: Extra files (other than the script) (string or list).
            NOTE: Hadoop copies the files into working directory (path errors!).
        jobconfs: Extra jobconf parameters (string or list)
        cmdenvs: Extra cmdenv parameters (string or list)
        hstreaming: The full hadoop streaming path to call.
        frozen_path: If True, copy_script is overriden to false, the .py
            extension is removed from script_path, and the value of frozen_path
            is added to files.
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
    # Remove .py extension if frozen
    if frozen_path and script_name.endswith('.py'):
        script_name = script_name[:-3]
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
            '"%s"'%(mapper)]
    if reducer:
        cmd += ['-reducer', 
                '"%s"'%(reducer)]
    else:
        cmd += ['-reducer', 
                'NONE']
    if combiner:
        cmd += ['-combiner', 
                '"%s"'%(combiner)]
    # Add files
    if isinstance(files, str):
        files = [files]
    if copy_script and frozen_path == None:
        files = list(files)
        files.append(script_path)
    if frozen_path:
        files = list(files)
        files.append(frozen_path)
        
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
    if compress_output:
        jobconfs.append('mapred.output.compress=true')
        jobconfs.append('mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec')
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
