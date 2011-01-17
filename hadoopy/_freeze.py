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

"""Makes hadoop scripts into unix executables (using cxfreeze)
The output directory can then be included in the job as a 'file' include

Example:
This will run 'myhadoopscript.py' on a hadoop cluster that doesn't have python
installed (in this example we don't require any special shared libraries).
freeze(script_path='myhadoopscript.py')
run_hadoop(in_name=input_path,
    out_name=output_path,
    script_path='myhadoopscript.py',
    frozen_path='frozen')

If you get errors when running on the target cluster, do the following.
1. If it is a libc error like /lib/libc.so.6: version GLIBC_2.7 not found
    then you will have to build this on a machine with <= version of the
    target libc version. (this is the worst problem to have)
2. If you get an error that a .so file is not found, then include it's path
    in shared_libs.
3. If a module is missing, then include it in modules.
4. Google it
"""

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import os
import shutil
import subprocess
import tarfile
import glob
import tempfile


def _wrap_string(s):
    if isinstance(s, str):
        return [s]
    return list(s)


def freeze(script_path, shared_libs=(), modules=(), remove_dir=False,
           target_dir='frozen', exclude_modules=('tcl', 'tk', 'Tkinter'),
           freeze_cmd='cxfreeze', pretend=False, verbose=False, extra='',
           **kw):
    """Wraps cxfreeze and provides an easy to use interface (see module doc).

    Args:
        script_path: Path to python script to be frozen.
        shared_libs: A sequence of additional shared library paths to include.
        modules: Additional modules to include.
        remove_dir: Will rm -r the target_dir when true (be careful)!
            (default is False)
        target_dir: Output directory where cxfreeze and this function output.
        exclude_modules: A sequence of modules to ignore
            (default is (tcl, tk, Tkinter))
        freeze_cmd: Path to cxfreeze (default is cxfreeze)
        pretend: If true, only build the command and return.
        verbose: If true, output to stdout all command results.
        extra: A string to be appended to the command, pass string args here

    Returns:
        The cxfreeze command called (string).

    Raises:
        subprocess.CalledProcessError: Cxfreeze error.
        OSError: Cxfreeze not found.
    """
    if remove_dir and not pretend:
        try:
            shutil.rmtree(target_dir)
        except OSError:
            pass
    shared_libs = _wrap_string(shared_libs)
    modules = _wrap_string(modules)
    exclude_modules = _wrap_string(exclude_modules)
    # Force utf8 encoding in modules
    modules += ['encodings.utf_8']
    # Run freeze for each script
    if exclude_modules:
        freeze_cmd += ' --exclude-modules=%s' % (','.join(exclude_modules))
    if target_dir:
        freeze_cmd += ' --target-dir=%s' % (target_dir)
    if modules:
        freeze_cmd += ' --include-modules=%s' % (','.join(modules))
    freeze_cmd += ' %s %s' % (extra, script_path)
    if not pretend:
        if verbose:
            print('HadooPY: Running[%s]' % (freeze_cmd))
        else:
            freeze_cmd += ' -s'
        subprocess.check_call(freeze_cmd.split())
        # Copy all of the extra shared libraries
        for shared_lib in shared_libs:
            shutil.copy(shared_lib, ''.join((target_dir, '/',
                                             os.path.basename(shared_lib))))
    return freeze_cmd


def freeze_to_tar(script_path, freeze_fn, extra, extra_files):
    """Freezes a script to a .tar or .tar.gz file

    The script contains all of the files at the root of the tar

    Args:
        script_path: Path to python script to be frozen.
        freeze_fn: Tar filename (must end in .tar or .tar.gz)
        extra: A string to be appended to the command, pass string args here
        extra_files: List of paths to add to the tar

    Raises:
        subprocess.CalledProcessError: Cxfreeze error.
        OSError: Cxfreeze not found.
        NameError: Tar must end in .tar or .tar.gz
    """
    freeze_dir = tempfile.mkdtemp()
    freeze(script_path, target_dir=freeze_dir, extra=extra)
    if freeze_fn.endswith('.tar.gz'):
        mode = 'w|gz'
    elif freeze_fn.endswith('.tar'):
        mode = 'w'
    else:
        shutil.rmtree(freeze_dir)
        raise NameError('[%s] must end in .tar or .tar.gz' % freeze_fn)
    fp = tarfile.open(freeze_fn, mode)
    for x in glob.glob('%s/*' % freeze_dir) + extra_files:
        fp.add(x, arcname=os.path.basename(x))
    fp.close()
    shutil.rmtree(freeze_dir)
