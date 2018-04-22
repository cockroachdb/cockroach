This directory contains unit tests for the script `release-notes.py`.

The provided `Makefile` runs all tests.

To run a single test invoke with:

```
bash ./testN.sh $PWD/../release-notes.py
```

Optionally, set the `PYTHON` env var to the path of a python 3
interpreter (useful e.g. if the base `python` command is not v3).

If the script changes and the reference output is not accurate any
more, you can re-generate a reference output as follows:

```
bash ./testN.sh $PWD/../release-notes.py rewrite
```

(i.e. provide one more positional argument)
