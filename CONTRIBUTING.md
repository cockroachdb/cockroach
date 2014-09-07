# Contributing to Cockroach

### Getting and building

A working [Go environment](http://golang.org/doc/code.html) and [prerequisites for building
RocksDB](https://github.com/cockroachdb/rocksdb/blob/master/INSTALL.md) are both presumed.
```bash
mkdir -p $GOPATH/src/github.com/cockroachdb/
cd $GOPATH/src/github.com/cockroachdb/
git clone git@github.com:cockroachdb/cockroach.git
cd cockroach
./bootstrap.sh
make
```

### Code review workflow

+ Create a local branch to do work on.

`git checkout -b andybons/update-readme`

+ Hack away and commit your changes locally using `git add` and `git commit`.

`git commit -a -m 'update CONTRIBUTING.md'`

+ When you’re ready for review, create a remote branch from your local branch.

`git push -u origin andybons/update-readme`

+ Then [create a pull request using GitHub’s UI](https://help.github.com/articles/creating-a-pull-request).
