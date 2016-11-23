# Contributing to Cockroach

### Getting and building

1.  Install the following prerequisites, as necessary:
  - A C++ compiler that supports C++11. Note that GCC prior to 6.0 doesn't
    work due to https://gcc.gnu.org/bugzilla/show_bug.cgi?id=48891
  - A Go environment with a recent 64-bit version of the toolchain. Note that
    the Makefile enforces the specific version required, as it is updated
    frequently.
  - Git 1.8+
  - Bash

2.  Get the CockroachDB code:

	```bash
	go get -d github.com/cockroachdb/cockroach
	cd $GOPATH/src/github.com/cockroachdb/cockroach
	```

3.  Run `make build`, `make test`, or anything else our Makefile offers. Note
that at least 4GB of RAM is required to build from source and run tests. Also,
the first time you run `make`, it can take some time to download and install
various dependencies. After running `make build`, the `cockroach` executable
will be in your current directory and can be run as shown in the
[README](README.md).

Note that if you edit a `.proto` or `.ts` file, you will need to manually
regenerate the associated `.pb.{go,cc,h}` or `.js` files using `go generate
./...`.

We advise to run `go generate` using our embedded Docker setup.
`build/builder.sh` is a wrapper script designed to make this convenient. You can
run `build/builder.sh env SKIP_BOOTSTRAP=0 go generate ./...` from the repository
root to get the intended result.

If you want to run it outside of Docker, `go generate` requires a collection
of Node.js modules which will be automatically installed into the project tree
(not globally).

If you plan on working on the UI, check out [the ui readme](pkg/ui).

To add or update a go dependency:

- see `vendor/README.md` for details on adding or updating dependencies
- run `go generate ./pkg/...` to update generated files.
- create a PR with all the changes

### Style guide

[Style Guide](STYLE.md)

### Code review workflow

+ All contributors need to sign the [Contributor License Agreement]
  (https://cla-assistant.io/cockroachdb/cockroach).

+ Create a local feature branch to do work on, ideally on one thing at a time.
  If you are working on your own fork, see [this tip]
  (http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be correct.

  `git checkout -b update-readme`

+ Hack away and commit your changes locally using `git add` and `git commit`.
  Remember to write tests! The following are helpful for running specific
  subsets of tests:

  ```bash
  make test
  # Run all tests in ./pkg/storage
  make test PKG=./pkg/storage
  # Run all kv tests matching `^TestFoo` with a timeout of 10s
  make test PKG=./pkg/kv TESTS='^TestFoo' TESTTIMEOUT=10s
  ```

  When you're ready to commit, be sure to write a Good Commit Message™. Consult
  https://github.com/erlang/otp/wiki/Writing-good-commit-messages if you're
  not sure what constitutes a Good Commit Message™.
  In addition to the general rules referenced above, please also prefix your
  commit subject line with the affected package, if one can easily be chosen.
  For example, the subject line of a commit mostly affecting the
  `server/serverpb` package might read: "server/serverpb: made great again".
  Commits which affect many packages as a result of a shared dependency change
  should probably begin their subjects with the name of the shared dependency.
  Finally, some commits may need to affect many packages in a way which does
  not point to a specific package; those commits may begin with "*:" or "all:"
  to indicate their reach.

+ Run the whole CI test suite locally: `./build/circle-local.sh`. This requires
  the Docker setup; if you don't have/want that,
  `go generate ./... && make check test testrace` is a good first approximation.

+ When you’re ready for review, groom your work: each commit should pass tests
  and contain a substantial (but not overwhelming) unit of work. You may also
  want to `git fetch origin` and run
  `git rebase -i --exec "make check test" origin/master` to make sure you're
  submitting your changes on top of the newest version of our code. Next, push
  to your fork:

  `git push -u <yourfork> update-readme`

+ Then [create a pull request using GitHub’s UI]
  (https://help.github.com/articles/creating-a-pull-request). If you know of
  another GitHub user particularly suited to reviewing your pull request, be
  sure to mention them in the pull request body. If you possess the necessary
  GitHub privileges, please also [assign them to the pull request using
  GitHub's UI] (https://help.github.com/articles/assigning-issues-and-pull-requests-to-other-github-users/).
  This will help focus and expedite the code review process.

+ If you get a test failure in CircleCI, check the Test Failure tab to see why
  the test failed. When the failure is logged in `excerpt.txt`, you can find
  the file from the Artifacts tab and see log messages. (You need to sign in to
  see the Artifacts tab.)

+ Address feedback by amending your commits. If your change contains multiple
  commits, address each piece of feedback by amending that commit to which the
  particular feedback is aimed. Wait (or ask) for new feedback on those
  commits if they are not straightforward. An `LGTM` ("looks good to me") by
  someone qualified is usually posted when you're free to go ahead and merge.
  Most new contributors aren't allowed to merge themselves; in that case, we'll
  do it for you.

### Debugging

Peeking into a running cluster can be done in several ways:

* the [net/trace](https://godoc.org/golang.org/x/net/trace) endpoint at
  `/debug/requests`.  It has a breakdown of the recent traced requests, in
  particularly slow ones. Two families are traced: `node` and `coord`, the
  former (and likely more interesting one) containing what happens inside of
  `Node`/`Store`/`Replica` and the other inside of the coordinator
  (`TxnCoordSender`).
* [pprof](https://golang.org/pkg/net/http/pprof/) gives us (among other things)
  heap and cpu profiles; [this golang blog post]
  (http://blog.golang.org/profiling-go-programs) explains it extremely well and
  [this one by Dmitry Vuykov]
  (https://software.intel.com/en-us/blogs/2014/05/10/debugging-performance-issues-in-go-programs)
  goes into even more detail. Two caveats: the `cockroach` binary passed to
  `pprof` must be the same as the one creating the profile (not true on OSX in
  acceptance tests!), and the HTTP client used by `pprof` doesn't simply
  swallow self-signed certs (relevant when using SSL). For the latter, a
  workaround of the form

  ```
  go tool pprof cockroach <(curl -k https://$(hostname):26257/debug/pprof/profile)
  ```

  will do the trick.

An easy way to locally run a workload against a cluster are the acceptance
tests. For example,

```bash
make acceptance TESTS='TestPut$$' TESTFLAGS='-v -d 1200s -l .' TESTTIMEOUT=1210s
```

runs the `Put` acceptance test for 20 minutes with logging (useful to look at
the stacktrace in case of a node dying). When it starts, all the relevant
commands for `pprof`, `trace` and logs are logged to allow for convenient
inspection of the cluster.
