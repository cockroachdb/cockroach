# Contributing to Cockroach

### Getting and building

### Assumed
 * A working GCC (on mac os x something like `xcode-select --install` will get you started)
 * [Go environment](http://golang.org/doc/code.html)
 * Git and Mercurial (for retrieving dependencies)

If you're on Mac OS X, [homebrew](http://brew.sh/) can be very helpful to fulfill these dependencies.

You can `go get -d github.com/cockroachdb/cockroach` and then run `./bootstrap.sh` or, alternatively,

```bash
mkdir -p $GOPATH/src/github.com/cockroachdb/
cd $GOPATH/src/github.com/cockroachdb/
git clone git@github.com:cockroachdb/cockroach.git
cd cockroach
./bootstrap.sh
```

Now you should be all set for `make build`, `make test` and everything else our Makefile has
to offer. When dependency versions change, run `glock sync github.com/cockroachdb/cockroach`
or re-run `bootstrap.sh`.

Note that if you edit a `.proto` file you will need to manually regenerate the associated
`.pb.{go,cc,h}` files using `go generate`.

To add or update a dependency, use `go get -u` and then
`glock save github.com/cockroachdb/cockroach` and commit the changes to the GLOCKFILE.

### Style guide
We're following the [Google Go Code Review](https://code.google.com/p/go-wiki/wiki/CodeReviewComments) fairly closely. In particular, you want to watch out for proper punctuation and capitalization and make sure that your lines stay well below 80 characters.

### Code review workflow

+ Create a local feature branch to do work on, ideally on one thing at a time.
  If you are working on your own fork, see 
  [this tip](http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be correct.

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

