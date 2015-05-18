# Contributing to Cockroach

### Getting and building

### Assumed
 * A working C++ compiler (on mac os x something like `xcode-select
   --install` will get you started). The compiler must support C++11
   (GCC 4.9+ and clang 3.6+ are known to work).
 * [Go environment](http://golang.org/doc/code.html). Currently a
   64-bit version of go is required.
 * Git and Mercurial (for retrieving dependencies).

If you're on Mac OS X, [homebrew](http://brew.sh/) can be very helpful to fulfill these dependencies.

You can `go get -d github.com/cockroachdb/cockroach` or, alternatively,

```bash
mkdir -p $GOPATH/src/github.com/cockroachdb/
cd $GOPATH/src/github.com/cockroachdb/
git clone git@github.com:cockroachdb/cockroach.git
cd cockroach
```

Now you should be all set for `make build`, `make test` and everything else our Makefile has to
offer. Note that the first time you run `make` various dependent libraries and tools will be
downloaded and installed which can be somewhat time consuming. Be patient.

Note that if you edit a `.proto` or `.ts` file, you will need to manually regenerate the associated `.pb.{go,cc,h}` or `.js` files using `go generate ./...`.
`go generate` requires the typescript transpiler `tsc`, which you can get from [typescript](http://www.typescriptlang.org/) or using `npm` with:
`npm install -g typescript` 
If you don't have npm, it comes with node. To get it via homebrew:
`brew install node`

To add or update a dependency:
- `go get -u` to update the dependencies or `go get {package}` to add a dependency
- `glock save github.com/cockroachdb/cockroach` to update the GLOCKFILE
- `make listdeps` and take the output of that and replace the dependancy list in build/devbase/godeps.sh
- create a PR with all the changes

### Style guide
We're following the [Google Go Code Review](https://code.google.com/p/go-wiki/wiki/CodeReviewComments) fairly closely. In particular, you want to watch out for proper punctuation and capitalization and make sure that your lines stay well below 80 characters.

### Code review workflow

+ All contributors need to sign the
  [Contributor License Agreement](https://www.clahub.com/agreements/cockroachdb/cockroach).

+ Create a local feature branch to do work on, ideally on one thing at a time.
  If you are working on your own fork, see
  [this tip](http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be correct.

`git checkout -b $USER/update-readme`

+ Hack away and commit your changes locally using `git add` and `git commit`.

`git commit -a -m 'update CONTRIBUTING.md'`

+ Run tests. It's usually enough to run `make test testrace`. You can also run `make acceptance` to have better test coverage. Running acceptance tests requires the Docker setup.

+ When you’re ready for review, create a remote branch from your local branch. You may want to `git fetch origin` and run `git rebase origin/master` on your local feature branch before.

`git push -u origin $USER/update-readme`

+ Then [create a pull request using GitHub’s UI](https://help.github.com/articles/creating-a-pull-request).

+ Address feedback in new commits. Wait (or ask) for new feedback on those commits if they are not straightforward.

+ Once ready to land your change, squash your commits. Where n is the number of commits in your branch, run
`git rebase -i HEAD~n`

 and subsequently update your remote (you will have to force the push, `git push -f $USER mybranch`). The pull request will update.

+ If you do not have write access to the repository and your pull request requires a manual merge, you may be asked to rebase again,
  `git fetch origin; git rebase -i origin/master` and update the PR again. Otherwise, you are free to merge your branch into origin/master directly or rebase first as you deem appropriate.

