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
import hashlib
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
    return p.returncode


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


def _md5_file(fn, block_size=1048576):
    """Builds the MD5 of a file block by block

    Args:
        fn: File path
        block_size: Size of the blocks to consider (default 1048576)

    Returns:
        File MD5
    """
    h = hashlib.md5()
    with open(fn) as fp:
        d = 1
        while d:
            d = fp.read(block_size)
            h.update(d)
    return h.hexdigest()


def freeze_script(script_path, temp_path='_hadoopy_temp'):
    """Freezes a script, puts it on hdfs, and gives you the path

    'frozen_tar_path' can be given to launch_frozen and it will use that
    instead of making its own, this is useful for repeated calls.  If a
    file with the same md5 already exists in the temp_path, it is used
    instead of putting a new copy there to avoid the file transfer.  The
    files are put into a temporary file based on the timestamp first, then
    moved to a location that is only a function of their md5 to prevent partial
    files.

    Args:
        script_path: Path to a hadoopy script
        temp_path: HDFS temporary path (default is '_hadoopy_temp')

    Returns:
        {'cmds': commands_ran, 'frozen_tar_path': frozen_tar_path}
    """
    tmp_frozen_tar_path = temp_path + '/%f.tar' % time.time()
    freeze_fp = tempfile.NamedTemporaryFile(suffix='.tar')
    cmds = hadoopy._freeze.freeze_to_tar(os.path.abspath(script_path), freeze_fp.name)
    md5 = _md5_file(freeze_fp.name)
    frozen_tar_path = temp_path + '/%s.tar' % md5
    if hadoopy.exists(frozen_tar_path):
        return {'cmds': cmds, 'frozen_tar_path': frozen_tar_path}
    hadoopy.put(freeze_fp.name, tmp_frozen_tar_path)
    try:
        hadoopy.mv(tmp_frozen_tar_path, frozen_tar_path)
    except IOError, e:
        if hadoopy.exists(frozen_tar_path):  # Check again
            return {'cmds': cmds, 'frozen_tar_path': frozen_tar_path}
        raise e
    return {'cmds': cmds, 'frozen_tar_path': frozen_tar_path}


def _freeze_config(force=False, verbose=False):
    from hadoopy.thirdparty.pyinstaller.PyInstaller import DEFAULT_CONFIGFILE
    cmds = []
    if force:
        try:
            os.remove(DEFAULT_CONFIGFILE)
        except OSError:
            pass
    if not os.path.exists(DEFAULT_CONFIGFILE):
        pyinst_path = '%s/thirdparty/pyinstaller' % __path__[0]
        cur_cmd = 'python -O %s/utils/Configure.py' % (pyinst_path)
        cmds.append(cur_cmd)
        _run(cur_cmd, verbose=verbose)
    return cmds


def freeze(script_path, target_dir='frozen', verbose=False, **kw):
    """Wraps pyinstaller and provides an easy to use interface

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
    orig_dir = os.path.abspath('.')
    script_path = os.path.abspath(script_path)
    try:
        os.chdir(target_dir)
        cmds += _freeze_config(verbose=verbose)
        pyinst_path = '%s/thirdparty/pyinstaller' % __path__[0]
        cur_cmd = 'python -O %s/pyinstaller.py %s --skip-configure' % (pyinst_path, script_path)
        cmds.append(cur_cmd)
        if _run(cur_cmd, verbose=verbose):  # If there is a problem, try removing the config and re-doing
            _freeze_config(verbose=verbose, force=True)
            _run(cur_cmd, verbose=verbose)
    finally:
        os.chdir(orig_dir)
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
    try:
        cmds = freeze(script_path, target_dir=freeze_dir)
        if freeze_fn.endswith('.tar.gz'):
            mode = 'w|gz'
        elif freeze_fn.endswith('.tar'):
            mode = 'w'
        else:
            raise NameError('[%s] must end in .tar or .tar.gz' % freeze_fn)
        fp = tarfile.open(freeze_fn, mode)
        proj_name = os.path.basename(script_path)
        proj_name = proj_name[:proj_name.rfind('.')]  # Remove extension
        for x in glob.glob('%s/dist/%s/*' % (freeze_dir, proj_name)) + extra_files:
            fp.add(x, arcname=os.path.basename(x))
        fp.close()
    finally:
        shutil.rmtree(freeze_dir)
    return cmds
