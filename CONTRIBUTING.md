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

+ Create a local feature branch to do work on, ideally on one thing at a time.

`git checkout -b andybons/update-readme`

+ Hack away and commit your changes locally using `git add` and `git commit`.

`git commit -a -m 'update CONTRIBUTING.md'`

+ When you’re ready for review, create a remote branch from your local branch. You may want to `git fetch origin` and run `git rebase origin/master` on your local feature branch before.

`git push -u origin andybons/update-readme`

+ Then [create a pull request using GitHub’s UI](https://help.github.com/articles/creating-a-pull-request).

+ Address feedback in new commits. Wait (or ask) for new feedback on those commits if they are not straightforward.

+ Once ready to land your change, squash your commits. Where n is the number of commits in your branch, run
`git rebase -i HEAD~n`

 and subsequently update your remote (you will have to force the push, `git push -f andybons mybranch`). The pull request will update.

+ If you do not have write access to the repository and your pull request requires a manual merge, you may be asked to rebase again,
  `git fetch origin; git rebase -i origin/master` and update the PR again. Otherwise, you are free to merge your branch into origin/master directly or rebase first as you deem appropriate.

