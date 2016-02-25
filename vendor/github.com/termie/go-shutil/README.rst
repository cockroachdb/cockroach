=========================================
High-level Filesystem Operations (for Go)
=========================================


A direct port of a few of the functions from Python's shutil package for
high-level filesystem operations.

This project pretty much only exists so that other people don't have to keep
re-writing this code in their projects, at this time we have been unable to
find any helpful packages for this in the stdlib or elsewhere.

We don't expect it to be perfect, just better than whatever your first draft
would have been. Patches welcome.

See also: https://docs.python.org/3.5/library/shutil.html

================
Functions So Far
================

We support Copy, CopyFile, CopyMode, and CopyTree. CopyStat would be nice if
anybody wants to write that. Also the other functions that might be useful in
the python library :D
