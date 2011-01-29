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
from . import __path__


def _wrap_string(s):
    if isinstance(s, str):
        return [s]
    return list(s)


def _run(path):
    print(path)
    subprocess.call(path.split())


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
        except IOError:
            shutil.copytree('%s/%s' % (src, file), '%s/%s' % (dst, file))


def freeze(script_path, target_dir='frozen', **kw):
    """Wraps pyinstaller and provides an easy to use interface

    This requires that the Configure.py script has been run (this is run in
    setup.py during installation)

    Args:
        script_path: Absolute path to python script to be frozen.

    Raises:
        subprocess.CalledProcessError: Freeze error.
        OSError: Freeze not found.
    """
    print('/\\%s%s Output%s/\\' % ('-' * 10, 'Pyinstaller', '-' * 10))
    pyinst_path = tempfile.mkdtemp()
    root_path = '%s/thirdparty/pyinstaller' % __path__[0]
    script_dir = os.path.dirname(script_path)
    _run(('python %s/Makespec.py -o %s -C %s/config.dat -p %s %s' %
          (root_path, pyinst_path, root_path, script_dir,
           script_path)))
    proj_name = os.path.basename(script_path)
    proj_name = proj_name[:proj_name.rfind('.')]  # Remove extension
    spec_path = '%s/%s.spec' % (pyinst_path, proj_name)
    _run(('python %s/Build.py -y -o %s -C %s/config.dat %s' %
          (root_path, pyinst_path, root_path, spec_path)))
    _copytree('%s/dist/%s' % (pyinst_path, proj_name), target_dir)
    shutil.rmtree(pyinst_path)
    print('\\/%s%s Output%s\\/' % ('-' * 10, 'Pyinstaller', '-' * 10))


def freeze_to_tar(script_path, freeze_fn, extra_files=None):
    """Freezes a script to a .tar or .tar.gz file

    The script contains all of the files at the root of the tar

    Args:
        script_path: Path to python script to be frozen.
        freeze_fn: Tar filename (must end in .tar or .tar.gz)
        extra_files: List of paths to add to the tar (default is None)

    Raises:
        subprocess.CalledProcessError: freeze error.
        OSError: freeze not found.
        NameError: Tar must end in .tar or .tar.gz
    """
    if not extra_files:
        extra_files = []
    freeze_dir = tempfile.mkdtemp()
    freeze(script_path, target_dir=freeze_dir)
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
