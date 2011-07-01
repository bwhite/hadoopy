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

"""Makes hadoop scripts into unix executables
"""

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import os
import shutil
import subprocess
import tarfile
import glob
import tempfile
import hadoopy
import time
from . import __path__


def _wrap_string(s):
    if isinstance(s, str):
        return [s]
    return list(s)


def _run(path, verbose=False):
    p = subprocess.Popen(path.split(), stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stderr, stdout = p.communicate()
    if verbose or p.returncode != 0:
        print('stdout[%s] stderr[%s]' % (stdout, stderr))


def _copytree(src, dst):
    """Similar to shutils.copytree, except that dst is already there
    """
    try:
        os.makedirs(dst)
    except OSError:
        pass  # It must already exist
    for file in os.listdir(src):
        try:
            shutil.copy2('%s/%s' % (src, file), '%s/%s' % (dst, file))
        except IOError, e:
            try:
                shutil.copytree('%s/%s' % (src, file), '%s/%s' % (dst, file))
            except OSError:  # Not a directory, reraise copy2 exception
                raise e


def freeze_script(script_path, temp_path='_hadoopy_temp'):
    """Freezes a script, puts it on hdfs, and gives you the path

    'frozen_tar_path' can be given to launch_frozen and it will use that
    instead of making its own, this is useful for repeated calls.

    Args:
        script_path: Path to a hadoopy script
        temp_path: HDFS temporary path (default is '_hadoopy_temp')

    Returns:
        {'cmds': commands_ran, 'frozen_tar_path': frozen_tar_path}
    """
    frozen_tar_path = temp_path + '/%f/_frozen.tar' % time.time()
    freeze_fp = tempfile.NamedTemporaryFile(suffix='.tar')
    cmds = hadoopy._freeze.freeze_to_tar(os.path.abspath(script_path), freeze_fp.name)
    hadoopy.put(freeze_fp.name, frozen_tar_path)
    return {'cmds': cmds, 'frozen_tar_path': frozen_tar_path}


def freeze(script_path, target_dir='frozen', verbose=False, **kw):
    """Wraps pyinstaller and provides an easy to use interface

    This requires that the Configure.py script has been run (this is run in
    setup.py during installation)

    Args:
        script_path: Absolute path to python script to be frozen.

    Returns:
        List of freeze commands ran

    Raises:
        subprocess.CalledProcessError: Freeze error.
        OSError: Freeze not found.
    """
    cmds = []
    if verbose:
        print('/\\%s%s Output%s/\\' % ('-' * 10, 'Pyinstaller', '-' * 10))
    pyinst_path = tempfile.mkdtemp()
    orig_pyinst_path = '%s/thirdparty/pyinstaller' % __path__[0]
    root_path = pyinst_path + '/pyinstaller'
    # Copy pyinstaller to the working directory as it assumes local paths
    shutil.copytree(orig_pyinst_path, root_path)
    script_dir = os.path.dirname(script_path)
    cur_cmd = 'python -O %s/Configure.py' % (root_path)
    cmds.append(cur_cmd)
    _run(cur_cmd, verbose=verbose)
    cur_cmd = 'python -O %s/Makespec.py -o %s -C %s/config.dat -p %s %s' % (root_path, pyinst_path, root_path, script_dir, script_path)
    cmds.append(cur_cmd)
    _run(cur_cmd, verbose=verbose)
    proj_name = os.path.basename(script_path)
    proj_name = proj_name[:proj_name.rfind('.')]  # Remove extension
    spec_path = '%s/%s.spec' % (pyinst_path, proj_name)
    cur_cmd = 'python -O %s/Build.py -y -o %s -C %s/config.dat %s' % (root_path, pyinst_path, root_path, spec_path)
    cmds.append(cur_cmd)
    _run(cur_cmd, verbose=verbose)
    _copytree('%s/dist/%s' % (pyinst_path, proj_name), target_dir)
    shutil.rmtree(pyinst_path)
    if verbose:
        print('\\/%s%s Output%s\\/' % ('-' * 10, 'Pyinstaller', '-' * 10))
    return cmds


def freeze_to_tar(script_path, freeze_fn, extra_files=None):
    """Freezes a script to a .tar or .tar.gz file

    The script contains all of the files at the root of the tar

    Args:
        script_path: Path to python script to be frozen.
        freeze_fn: Tar filename (must end in .tar or .tar.gz)
        extra_files: List of paths to add to the tar (default is None)

    Returns:
        List of freeze commands ran

    Raises:
        subprocess.CalledProcessError: freeze error.
        OSError: freeze not found.
        NameError: Tar must end in .tar or .tar.gz
    """
    if not extra_files:
        extra_files = []
    freeze_dir = tempfile.mkdtemp()
    cmds = freeze(script_path, target_dir=freeze_dir)
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
    return cmds
