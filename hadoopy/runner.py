import subprocess
import re


def _script_name_from_path(script_path):
    return re.search(r'([^/]+$)', script_path).group(1)


def _find_hstreaming():
    p = subprocess.Popen('find / -name hadoop*streaming.jar'.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.communicate()[0].split('\n')[0]


def run_hadoop(in_name, out_name, script_path, map=True, reduce=True,
               combine=False, files=[], jobconfs=[], cmdenvs=[],
               compress_input=False, compress_output=False,
               copy_script=True, in_map_reduce=False, disable_log=False,
               hstreaming=None, name=None, use_typedbytes=True, frozen_path=None,
               use_seqoutput=True, use_autoinput=True):
    """Run Hadoop given the parameters

    Keyword Arguments:
    in_name -- Input path (string or list)
    out_name -- Output path
    script_path -- Path to the script (e.g., script.py)
    map -- If True, the mapper is "script.py map".If string, the mapper is the value
    reduce -- If True, the reducer is "script.py reduce".  If string, the reducer is the value
    combiner -- If True, the reducer is "script.py combine". If string, the cominer is the value
    copy_script -- If True, the script is added to the files list.
    files - Extra files (other than the script) (string or list).  NOTE: Hadoop copies the files into working directory (path errors!).
    jobconfs - Extra jobconf parameters (e.g., mapred.reduce.tasks=1) (string or list)
    cmdenvs - Extra cmdenv parameters (string or list)
    hstreaming - The full hadoop streaming path to cal
    disable_log - If True, sets hadoop.job.history.user.location=None.
    frozen_path - If True, copy_script is overriden to false and the value of frozen_path is added to files.
    use_typedbytes - If True, use typedbytes IO. (default True)
    use_seqoutput - True (default), output sequence file. If False, output is text.
    use_autoinput - If True, sets the input format to auto.
    """
    try:
        hadoop_cmd = 'hadoop jar ' + hstreaming
    except TypeError:
        hadoop_cmd = 'hadoop jar ' + _find_hstreaming()
    script_name = _script_name_from_path(script_path)
    if map == True:
        map = ' '.join((script_name, 'map'))
    if reduce == True:
        reduce = ' '.join((script_name, 'reduce'))
    if in_map_reduce:
        map = '%s | sort | %s' % (map, reduce)
    if combine == True:
        combine = ' '.join((script_name, 'combine'))
    cmd = ('%s -output %s'%(hadoop_cmd, out_name)).split()
    # Add inputs
    if isinstance(in_name, str):
        in_name = [in_name]
    for f in in_name:
        cmd += ['-input', f]        
    # Add mapper/reducer
    cmd += ['-mapper',
            '"%s"'%(map)]
    if reduce:
        cmd += ['-reducer', 
                '"%s"'%(reduce)]
    else:
        cmd += ['-reducer', 
                'NONE']
    if combine:
        cmd += ['-combiner', 
                '"%s"'%(combine)]
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
    if compress_input:
        jobconfs.append('stream.recordreader.compression=gzip')
    if compress_output:
        jobconfs.append('mapred.output.compress=true')
        jobconfs.append('mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec')
    if disable_log:
        jobconfs += ['hadoop.job.history.user.location=None']
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
    print('HadooPY: Running[%s]' % (' '.join(cmd)))
    subprocess.Popen(cmd).wait()
