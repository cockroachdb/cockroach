# Contributing to Cockroach

### Getting and building

1.  Install the following prerequisites, as necessary:
	- A C++ compiler that supports C++11 (GCC 4.9+ and clang 3.6+ are
	  known to work). On Mac OS X, Xcode should suffice.
	- A [Go environment](http://golang.org/doc/code.html) with a
	  64-bit version of Go 1.6. You can download the
	  [Go binary](https://golang.org/dl/) directly from the official
	  site. On OS X, you can also use [homebrew](http://brew.sh):
	  `brew install go`. Be sure to set the `$GOPATH` and `$PATH`
	  environment variables as described
	  [here](https://golang.org/doc/code.html#GOPATH).
	- Git 1.8+

2.  Get the CockroachDB code:

	```bash
	go get -d github.com/cockroachdb/cockroach
	cd $GOPATH/src/github.com/cockroachdb/cockroach
	```

3.  Run `make build`, `make test`, or anything else our Makefile
	offers. Note that the first time you run `make`, it can take some
	time to download and install various dependencies.

Note that if you edit a `.proto` or `.ts` file, you will need to
manually regenerate the associated `.pb.{go,cc,h}` or `.js` files
using `go generate ./...`.

We advise to run `go generate` using our embedded Docker
setup. `build/builder.sh` is a wrapper script designed to make this
convenient. You can run `build/builder.sh go generate ./...` from the
repository root to get the intended result.

If you want to run it outside of Docker, `go generate` requires a
collection of Node.js modules which are installed via npm.

If you plan on working on the UI, check out [the ui readme](ui).

To add or update a go dependency:
- `(cd $GOPATH/src && go get -u ./...)` to update the dependencies or
  `go get {package}` to add a dependency
- `glock save github.com/cockroachdb/cockroach` to update the
  GLOCKFILE
- `go generate ./...` to update generated files -- prefer `go generate
  ./the-updated-package` instead of `...` when possible to avoid
  re-generating files in directories where you haven't made any
  changes.
- create a PR with all the changes

### Style guide

[Style Guide](STYLE.md)

### Code review workflow

+ All contributors need to sign the
  [Contributor License Agreement](https://cla-assistant.io/cockroachdb/cockroach).

+ Create a local feature branch to do work on, ideally on one thing at a time.
  If you are working on your own fork, see
  [this tip](http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be correct.

  `git checkout -b update-readme`

+ Hack away and commit your changes locally using `git add` and `git
  commit`. Remember to write tests! The following are helpful for
  running specific subsets of tests:

  ```bash
  make test
  # Run all tests in ./storage
  make test PKG=./storage
  # Run all kv tests matching `^TestFoo` with a timeout of 10s
  make test PKG=./kv TESTS='^TestFoo' TESTTIMEOUT=10s
  ```

  When you're ready to commit, do just that with a succinct title and
  informative message. For example,

  ```bash
  $ git commit
  > 'update CONTRIBUTING.md
  >
  > Added details on running specific tests via `make`, and
  > the CircleCI-equivalent test suite.
  >
  > Fixed some formatting.'
  ```

+ Run the whole CI test suite locally: `./build/circle-local.sh`. This
  requires the Docker setup; if you don't have/want that, `go generate
  ./... && make check test testrace` is a good first approximation.

+ When you’re ready for review, groom your work: each commit should
  pass tests and contain a substantial (but not overwhelming) unit of
  work. You may also want to `git fetch origin` and run `git rebase -i
  --exec "make check test" origin/master` to make sure you're
  submitting your changes on top of the newest version of our
  code. Next, push to your fork:

  `git push -u <yourfork> update-readme`

+ Then
  [create a pull request using GitHub’s UI](https://help.github.com/articles/creating-a-pull-request).

+ If you get a test failure in CircleCI, check the Test Failure tab to
  see why the test failed. When the failure is logged in
  `excerpt.txt`, you can find the file from the Artifacts tab and see
  log messages. (You need to sign in to see the Artifacts tab.)

+ Address feedback in new commits. Wait (or ask) for new feedback on
  those commits if they are not straightforward. An `LGTM` ("looks
  good to me") by someone qualified is usually posted when you're free
  to go ahead and merge. Most new contributors aren't allowed to merge
  themselves; in that case, we'll do it for you. You may also be asked
  to re-groom your commits.


### Debugging

Peeking into a running cluster can be done in several ways:

* the [net/trace](https://godoc.org/golang.org/x/net/trace) endpoint
  at `/debug/requests`.  It has a breakdown of the recent traced
  requests, in particularly slow ones. Two families are traced: `node`
  and `coord`, the former (and likely more interesting one) containing
  what happens inside of `Node`/`Store`/`Replica` and the other inside
  of the coordinator (`TxnCoordSender`).
* [pprof](https://golang.org/pkg/net/http/pprof/) gives us (among
  other things) heap and cpu profiles;
  [this golang blog post](http://blog.golang.org/profiling-go-programs)
  explains it extremely well and
  [this one by Dmitry Vuykov](https://software.intel.com/en-us/blogs/2014/05/10/debugging-performance-issues-in-go-programs)
  goes into even more detail. Two caveats: the `cockroach` binary
  passed to `pprof` must be the same as the one creating the profile
  (not true on OSX in acceptance tests!), and the HTTP client used by
  `pprof` doesn't simply swallow self-signed certs (relevant when
  using SSL). For the latter, a workaround of the form

  ```
  go tool pprof cockroach <(curl -k https://$(hostname):26257/debug/pprof/profile)
  ```

  will do the trick.

An easy way to locally run a workload against a cluster are the
acceptance tests.  For example,

```bash
make acceptance TESTS='TestPut$$' TESTFLAGS='-v -d 1200s -l .' TESTTIMEOUT=1210s
```

runs the `Put` acceptance test for 20 minutes with logging (useful to
look at the stacktrace in case of a node dying). When it starts, all
the relevant commands for `pprof`, `trace` and logs are logged to
allow for convenient inspection of the cluster.
