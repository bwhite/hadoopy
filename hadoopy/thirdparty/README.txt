Pyinstaller
b14bdd9df6c7dadc2776c3dfe18431ba  trunk-1899.zip http://bw-school.s3.amazonaws.com/trunk-1899.zip

Modifications
1. pyinstaller/PyInstaller/__init__.py: Allow import from non-parent
2. pyinstaller/__init__.py: Added file
3. pyinstaller/source/linux/utils.c: Fix handling of signals
4. Removed buildtests, doc, e2etests, examples, and Windows support files/directories


diff -r pyinstaller/ trunk | grep -v \.pyc

Only in trunk: buildtests
Only in trunk: doc
Only in trunk: e2etests
Only in trunk: examples
Only in pyinstaller/: __init__.py
diff -r pyinstaller//PyInstaller/__init__.py trunk/PyInstaller/__init__.py
34,37c34
< try:
<     from PyInstaller import lib
< except ImportError:
<     import lib
---
> from PyInstaller import lib
39,44c36,38
< try:
<     from PyInstaller import compat
<     from PyInstaller.utils import svn
< except ImportError:
<     import compat
<     from utils import svn
---
> 
> from PyInstaller import compat
> from PyInstaller.utils import svn
Only in pyinstaller//PyInstaller: __init__.py~
diff -r pyinstaller//source/linux/utils.c trunk/source/linux/utils.c
220,226c220,221
<     if (WIFEXITED(rc))
<         return WEXITSTATUS(rc);
<     if (WIFSIGNALED(rc)) {
<         raise(WTERMSIG(rc));
<         return 1;
<     }
<     return 1;
---
> 
>     return WEXITSTATUS(rc);
Only in trunk/support/loader: Windows-32bit
Only in trunk/support/loader: Windows-64bit
