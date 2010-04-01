#!/usr/bin/env python
"""Makes hadoop scripts into unix executables (using cxfreeze)
The output directory can then be included in the job as a 'file' include
"""

__authors__ = ['"Brandyn White" <bwhite@dappervision.com>']


import os
import shutil
import subprocess


def freeze(script, shared_libs=(), modules=(), remove_dir=False,
           target_dir='frozen', exclude_modules=('tcl', 'tk', 'Tkinter'),
           cmd='cxfreeze'):
    """Freeze a script and perform operations on the output dir
    Args:
        script: Path to python script to be frozen.
        shared_libs: A sequence of additional shared library paths to include.
        modules: Additional modules to include.
        remove_dir: Will rm -r the target_dir when true (be careful)!
            (default is False)
        target_dir: Output directory where cxfreeze and this function output.
        exclude_modules: A sequence of modules to ignore
            (default is (tcl, tk, Tkinter))
        cmd: Path to cxfreeze (default is cxfreeze)
    """
    if remove_dir:
        try:
            shutil.rmtree(target_dir)
        except OSError:
            pass
    # Run freeze for each script
    if exclude_modules:
        cmd += ' --exclude-modules=%s' % (','.join(exclude_modules))
    if target_dir:
        cmd += ' --target-dir=%s' % (target_dir)
    if modules:
        cmd += ' --include-modules=%s' % (','.join(modules))
    cmd += ' %s' % (script)
    subprocess.check_call(cmd.split())
    # Copy all of the extra shared libraries
    for shared_lib in shared_libs:
        print(shared_lib)
        shutil.copy(shared_lib, ''.join((target_dir, '/',
                                         os.path.basename(shared_lib))))
