import hadoopy
import logging
import os
import multiprocessing
import sys
import select
import subprocess
import tempfile
import shutil
import contextlib
import fcntl


def _local_reader(q_r_fd, q_w_fd, in_r_fd, in_w_fd, out_r_fd, out_w_fd):
    """
    caller(q_r_fd, in_w_fd)
    reader(out_r_fd, q_w_fd)
    worker(in_r_fd, out_w_fd)
    """
    os.close(q_r_fd)
    os.close(in_w_fd)
    os.close(in_r_fd)
    os.close(out_w_fd)
    flr = fcntl.fcntl(out_r_fd, fcntl.F_GETFL)
    fcntl.fcntl(out_r_fd, fcntl.F_SETFL, flr | os.O_NONBLOCK)
    flw = fcntl.fcntl(q_w_fd, fcntl.F_GETFL)
    fcntl.fcntl(q_w_fd, fcntl.F_SETFL, flw | os.O_NONBLOCK)
    data = []

    def write_data():
        while data:
            d = data[0]
            try:
                l = os.write(q_w_fd, d)
            except OSError:  # Too much data in pipe
                break
            if l != len(d):  # Too much data in pipe
                data[0] = d[l:]
                break
            del data[0]
    while True:
        try:
            d = os.read(out_r_fd, 1048576)  # 1MB Max read per iter
            if not d:  # We are done
                break
            data.append(d)
        except OSError:
            pass
        write_data()
    # Re-enable blocking on write socket, as we have read everything
    fcntl.fcntl(q_w_fd, fcntl.F_SETFL, flw)
    while data:
        write_data()
    os.close(q_w_fd)


@contextlib.contextmanager
def chdir(path):
    orig_pwd = os.path.abspath('.')
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(orig_pwd)


class LocalTask(object):

    def __init__(self, script_path, task, max_input, pipe, python_cmd, files, remove_tempdir):
        self.remove_tempdir = remove_tempdir
        self.temp_dir = tempfile.mkdtemp()
        self.script_path = script_path
        self.task = task
        self.max_input = max_input if task == 'map' else None
        self.pipe = pipe
        self.python_cmd = python_cmd
        self.files = files
        self._setup()

    def _setup(self):
        with chdir(self.temp_dir):
            if self.files:
                for f in self.files:
                    shutil.copy(f, os.path.basename(f))
            hadoopy._runner._make_script_executable(os.path.basename(self.script_path))

    def __del__(self):
        if self.remove_tempdir:
            shutil.rmtree(self.temp_dir)
        else:
            logging.warn('Temporary directory not removed[%s]' % self.temp_dir)

    def run_task(self, kvs, env):
        sys.stdout.flush()
        task = 'pipe %s' % self.task if self.pipe else self.task
        in_r_fd, in_w_fd = os.pipe()
        out_r_fd, out_w_fd = os.pipe()
        q_r_fd, q_w_fd = os.pipe()
        cmd = ('%s %s %s' % (self.python_cmd, self.script_path, task)).split()
        a = os.fdopen(in_r_fd, 'r')
        b = os.fdopen(out_w_fd, 'w')
        reader_process = multiprocessing.Process(target=_local_reader, args=(q_r_fd, q_w_fd, in_r_fd, in_w_fd, out_r_fd, out_w_fd))
        reader_process.start()
        os.close(out_r_fd)
        os.close(q_w_fd)
        try:
            with chdir(self.temp_dir):
                p = subprocess.Popen(cmd,
                                     stdin=a,
                                     stdout=b,
                                     close_fds=True,
                                     env=env)
            a.close()
            b.close()
            with hadoopy.TypedBytesFile(read_fd=q_r_fd) as tbfp_r:
                with hadoopy.TypedBytesFile(write_fd=in_w_fd, flush_writes=True) as tbfp_w:
                    for num, kv in enumerate(kvs):
                        if self.max_input is not None and self.max_input <= num:
                            break
                        while True:
                            if reader_process.exitcode:
                                raise EOFError('Reader process died[%s]' % reader_process.exitcode)
                            r, w, _ = select.select([q_r_fd], [in_w_fd], [], 0)
                            if r:  # If data is available to be read, than get it
                                yield tbfp_r.next()
                            elif w and kv is not None:
                                tbfp_w.write(kv)
                                kv = None
                            elif w and kv is None:  # We can write but have already written
                                break
                # Get any remaining values
                while True:
                    if reader_process.exitcode:
                        raise EOFError('Reader process died[%s]' % reader_process.exitcode)
                    try:
                        yield tbfp_r.next()
                    except EOFError:
                        break
        finally:
            reader_process.join()
            p.kill()
            p.wait()


def launch_local(in_name, out_name, script_path, max_input=None,
                 files=(), cmdenvs=(), pipe=True, python_cmd='python', remove_tempdir=True,
                 **kw):
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
    :param max_input: Maximum number of Mapper inputs, None (default) then unlimited.
    :param files: Extra files (other than the script) (iterator).  NOTE: Hadoop copies the files into working directory
    :param cmdenvs: Extra cmdenv parameters (iterator)
    :param pipe: If true (default) then call user code through a pipe to isolate it and stop bugs when printing to stdout.  See project docs.
    :param python_cmd: The python command to use. The default is "python".  Can be used to override the system default python, e.g. python_cmd = "python2.6"
    :param remove_tempdir: If True (default), then rmtree the temporary dir, else print its location.  Useful if you need to see temporary files or how input files are copied.
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
    if isinstance(files, (str, unicode)) or isinstance(cmdenvs, (str, unicode)) or ('cmdenvs' in kw and isinstance(kw['cmdenvs'], (str, unicode))):
        raise TypeError('files and cmdenvs must be iterators of strings and not strings!')
    cmdenvs = hadoopy._runner._listeq_to_dict(cmdenvs)
    logging.info('Local[%s]' % script_path)
    script_info = hadoopy._runner._parse_info(script_path, python_cmd)
    if not files:
        files = []
    else:
        files = list(files)
    files.append(script_path)
    files = [os.path.abspath(f) for f in files]
    # Setup env
    env = dict(os.environ)
    env['stream_map_input'] = 'typedbytes'
    env.update(cmdenvs)
    if isinstance(in_name, (str, unicode)) or (in_name and isinstance(in_name, (list, tuple)) and isinstance(in_name[0], (str, unicode))):
        in_kvs = hadoopy.readtb(in_name)
    else:
        in_kvs = in_name
    if 'reduce' in script_info['tasks']:
        kvs = list(LocalTask(script_path, 'map', max_input, pipe,
                             python_cmd, files, remove_tempdir).run_task(in_kvs, env))
        if 'combine' in script_info['tasks']:
            kvs = hadoopy.Test.sort_kv(kvs)
            kvs = list(LocalTask(script_path, 'combine', max_input, pipe,
                                 python_cmd, files, remove_tempdir).run_task(kvs, env))
        kvs = hadoopy.Test.sort_kv(kvs)
        kvs = LocalTask(script_path, 'reduce', max_input, pipe,
                        python_cmd, files, remove_tempdir).run_task(kvs, env)
    else:
        kvs = LocalTask(script_path, 'map', max_input, pipe,
                        python_cmd, files, remove_tempdir).run_task(in_kvs, env)
    out = {}
    if out_name is not None:
        hadoopy.writetb(out_name, kvs)
        out['output'] = hadoopy.readtb(out_name)
    else:
        out['output'] = kvs
    return out

