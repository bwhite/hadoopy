import subprocess
import re


def script_name_from_path(script_path):
    return re.search(r'([^/]+$)', script_path).group(1)

def run_hadoop(in_name, out_name, script_path, map=True, reduce=True,
               combine=False, files=[], jobconfs=[], cmdenvs=[],
               hadoop_cmd='hadoop jar /usr/lib/hadoop/contrib/streaming/' \
                   'hadoop-0.18.3-6cloudera0.3.0-streaming.jar'):
    """Run Hadoop given the parameters

    Keyword Arguments:
    in_name -- Input path (string or list)
    out_name -- Output path
    script_path -- Path to the script (e.g., script.py)
    map -- If True, the mapper is "script.py map".If string, the mapper is the value of map
    reduce -- If True, the reducer is "script.py reduce".  If string, the reducer is the value of reduce
    combiner -- If True, (assumes there is a map and reduce), uses "script.py map | sort | script.py reduce" as the mapper
    files - Extra files (other than the script) (string or list).  NOTE: Hadoop copies the files into working directory (path errors!).
    jobconfs - Extra jobconf parameters (e.g., mapred.reduce.tasks=1) (string or list)
    cmdenvs - Extra cmdenv parameters (string or list)
    hadoop_cmd - The full hadoop streaming command to call (TODO This should be automatically detected)
    """
    script_name = script_name_from_path(script_path)
    if map == True:
        map = ' '.join((script_name, 'map'))
    if reduce == True:
        reduce = ' '.join((script_name, 'reduce'))
    if combine == True:
        map = '%s | sort | %s'%(map, reduce)
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
    # Add files
    if isinstance(files, str):
        files = [files, script_path]
    else:
        files.append(script_path)
    for f in files:
        cmd += ['-file', f]
    # Add jobconfs
    if isinstance(jobconfs, str):
        jobconfs = [jobconfs]
    for jobconf in jobconfs:
        cmd += ['-jobconf', jobconf]
    # Add cmdenv
    if isinstance(cmdenvs, str):
        cmdenvs = [cmdenvs]
    for cmdenv in cmdenvs:
        cmd += ['-cmdenv', cmdenv]
    # Run command and wait till it has completed
    print(cmd)
    subprocess.Popen(cmd).wait()
