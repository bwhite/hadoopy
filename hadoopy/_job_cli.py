import sys
import hadoopy
import argparse
import os
import json
import subprocess
from _hadoopy_main import HadoopyTask


def run_freeze(tar_path, Z):
    hadoopy._freeze.freeze_to_tar(script_path=os.path.abspath(sys.argv[0]),
                                  freeze_fn=tar_path,
                                  extra_files=Z)


def _get_execution_name():
    """Return a command that can be called to run the script"""
    return sys.argv[0]


def run_pipe(command):
    cmd = '%s %s %d %d' % (_get_execution_name(),
                           command,
                           os.dup(sys.stdin.fileno()),
                           os.dup(sys.stdout.fileno()))
    slave_stdin = open('/dev/null', 'r')
    slave_stdout = os.fdopen(os.dup(sys.stderr.fileno()), 'w')
    retcode = 0
    try:
        retcode = subprocess.call(cmd.split(), stdout=slave_stdout, stdin=slave_stdin)
    except OSError:  # If we can't find the file, check the local dir
        retcode = subprocess.call(('./' + cmd).split(), stdout=slave_stdout, stdin=slave_stdin)
    if retcode:
        sys.exit(retcode)


def run_info(mapper, reducer, combiner, kw):
    tasks = []
    if mapper:
        tasks.append('map')
    if reducer:
        tasks.append('reduce')
    if combiner:
        tasks.append('combine')
    info = dict(kw)
    info['tasks'] = tasks
    print(json.dumps(info))


def change_dir():
    """Change the local directory if the HADOOPY_CHDIR environmental variable is provided"""
    try:
        d = os.environ['HADOOPY_CHDIR']
        sys.stderr.write('HADOOPY: Trying to chdir to [%s]\n' % d)
    except KeyError:
        pass
    else:
        try:
            os.chdir(d)
        except OSError:
            sys.stderr.write('HADOOPY: Failed to chdir to [%s]\n' % d)


def run_task(mapper, reducer, combiner, command, read_fd=None, write_fd=None):
    change_dir()
    return HadoopyTask(mapper, reducer, combiner, command, read_fd, write_fd).run()


def disable_stdout_buffering():
    """This turns off stdout buffering so that outputs are immediately
    materialized and log messages show up before the program exits"""
    stdout_orig = sys.stdout
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    # NOTE(brandyn): This removes the original stdout
    return stdout_orig


def run(mapper=None, reducer=None, combiner=None, **kw):
    """Hadoopy entrance function

    This is to be called in all Hadoopy job's.  Handles arguments passed in,
    calls the provided functions with input, and stores the output.

    TypedBytes are used if the following is True
    os.environ['stream_map_input'] == 'typedbytes'

    It is *highly* recommended that TypedBytes be used for all non-trivial
    tasks.  Keep in mind that the semantics of what you can safely emit from
    your functions is limited when using Text (i.e., no \\t or \\n).  You can use
    the base64 module to ensure that your output is clean.

    If the HADOOPY_CHDIR environmental variable is set, this will immediately
    change the working directory to the one specified.  This is useful if your
    data is provided in an archive but your program assumes it is in that
    directory.

    As hadoop streaming relies on stdin/stdout/stderr for communication,
    anything that outputs on them in an unexpected way (especially stdout) will
    break the pipe on the Java side and can potentially cause data errors.  To
    fix this problem, hadoopy allows file descriptors (integers) to be provided
    to each task.  These will be used instead of stdin/stdout by hadoopy.  This
    is designed to combine with the 'pipe' command.

    To use the pipe functionality, instead of using
    `your_script.py map` use `your_script.py pipe map`
    which will call the script as a subprocess and use the read_fd/write_fd
    command line arguments for communication.  This isolates your script and
    eliminates the largest source of errors when using hadoop streaming.

    The pipe functionality has the following semantics
    stdin: Always an empty file
    stdout: Redirected to stderr (which is visible in the hadoop log)
    stderr: Kept as stderr
    read_fd: File descriptor that points to the true stdin
    write_fd: File descriptor that points to the true stdout

    | **Command Interface**
    | The command line switches added to your script (e.g., script.py) are

    python script.py *map* (read_fd) (write_fd)
        Use the provided mapper, optional read_fd/write_fd.
    python script.py *reduce* (read_fd) (write_fd)
        Use the provided reducer, optional read_fd/write_fd.
    python script.py *combine* (read_fd) (write_fd)
        Use the provided combiner, optional read_fd/write_fd.
    python script.py *freeze* <tar_path> <-Z add_file0 -Z add_file1...>
        Freeze the script to a tar file specified by <tar_path>.  The extension
        may be .tar or .tar.gz.  All files are placed in the root of the tar.
        Files specified with -Z will be added to the tar root.
    python script.py info
        Prints a json object containing 'tasks' which is a list of tasks which
        can include 'map', 'combine', and 'reduce'.  Also contains 'doc' which is
        the provided documentation through the doc argument to the run function.
        The tasks correspond to provided inputs to the run function.

    | **Specification of mapper/reducer/combiner**
    | Input Key/Value Types
    |     For TypedBytes/SequenceFileInputFormat, the Key/Value are the decoded TypedBytes
    |     For TextInputFormat, the Key is a byte offset (int) and the Value is a line without the newline (string)
    |
    | Output Key/Value Types
    |     For TypedBytes, anything Pickle-able can be used
    |     For Text, types are converted to string.  Note that neither may contain \\t or \\n as these are used in the encoding.  Output is key\\tvalue\\n
    |
    | Expected arguments
    |     mapper(key, value) or mapper.map(key, value)
    |     reducer(key, values) or reducer.reduce(key, values)
    |     combiner(key, values) or combiner.reduce(key, values)
    |
    | Optional methods
    |     func.configure(): Called before any input read.  Returns None.
    |     func.close():  Called after all input read.  Returns None or Iterator of (key, value)
    |
    | Expected return
    |     None or Iterator of (key, value)

    :param mapper: Function or class following the above spec
    :param reducer: Function or class following the above spec
    :param combiner: Function or class following the above spec
    :param doc: If specified, on error print this and call sys.exit(1)
    :rtype: True on error, else False (may not return if doc is set and
        there is an error)
    """
    script_path = os.path.abspath(__file__)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='Job Commands (additional help available inside each)')

    parser_freeze = subparsers.add_parser('freeze', help='Freeze the script to a tar file.')
    parser_freeze.add_argument('tar_path', help='Path to .tar or .tar.gz file.')
    parser_freeze.add_argument('-Z', help='Path to a file to be placed in the tar root (may be repeated for many files).', action='append')
    parser_freeze.set_defaults(func=run_freeze)

    parser_info = subparsers.add_parser('info', help='Display job info as JSON')
    parser_info.set_defaults(func=lambda : run_info(mapper, reducer, combiner, kw))

    parser_launch_frozen = subparsers.add_parser('launch_frozen', help='Run Hadoop job (freezes script)')
    parser_launch_frozen.add_argument('in_name', help='Input HDFS path')
    parser_launch_frozen.add_argument('out_name', help='Output HDFS path')
    parser_launch_frozen.set_defaults(func=lambda *args, **kw: hadoopy.launch_frozen(*args, script_path=script_path, **kw))

    parser_pipe = subparsers.add_parser('pipe', help='Internal: Run map/combine/reduce task using "pipe hopping" to make stdout redirect to stderr.')
    parser_pipe.add_argument('command', help='Command to run', choices=('map', 'reduce', 'combine'))
    parser_pipe.set_defaults(func=run_pipe)

    parser_map = subparsers.add_parser('map', help='Internal: Run map task.')
    parser_map.add_argument('read_fd', type=int, help='Read file descriptor', nargs='?')
    parser_map.add_argument('write_fd', type=int, help='Write file descriptor', nargs='?')
    parser_map.set_defaults(func=lambda **y: run_task(mapper, reducer, combiner, command='map', **y))

    parser_combine = subparsers.add_parser('combine', help='Internal: Run combine task.')
    parser_combine.add_argument('read_fd', type=int, help='Read file descriptor', nargs='?')
    parser_combine.add_argument('write_fd', type=int, help='Write file descriptor', nargs='?')
    parser_combine.set_defaults(func=lambda **y: run_task(mapper, reducer, combiner, command='combine', **y))

    parser_reduce = subparsers.add_parser('reduce', help='Internal: Run reduce task.')
    parser_reduce.add_argument('read_fd', type=int, help='Read file descriptor', nargs='?')
    parser_reduce.add_argument('write_fd', type=int, help='Write file descriptor', nargs='?')
    parser_reduce.set_defaults(func=lambda **y: run_task(mapper, reducer, combiner, command='reduce', **y))

    args = vars(parser.parse_args())
    # Call function with all arguments except for itself
    func = args['func']
    del args['func']
    ret = func(**args)
    # Handle return code
    if ret and 'doc' in kw:
        print_doc_quit(kw['doc'])
    return bool(ret)


def print_doc_quit(doc):
    if doc is not None:
        print(doc)
    sys.exit(1)


def job_cli():
    pass
