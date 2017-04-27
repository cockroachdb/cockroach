# Contributing to Cockroach

## Getting and Building

1.  Install the following prerequisites, as necessary:
  - A C++ compiler that supports C++11. Note that GCC prior to 6.0 doesn't
    work due to https://gcc.gnu.org/bugzilla/show_bug.cgi?id=48891
  - A Go environment with a recent 64-bit version of the toolchain. Note that
    the Makefile enforces the specific version required, as it is updated
    frequently.
  - Git 1.8+
  - Bash (4+ is preferred)
  - GNU Make (3.81+ is known to work)
  - CMake 2.8.12+
  - XZ Utils (5.2.3+ is known to work), except on macOS, where xz support is
    built in to `tar`.
  - Optional: NodeJS 6.x and Yarn 0.22.0+. Required when compiling protocol
    buffers.

  Note that at least 4GB of RAM is required to build from source and run tests.

2.  Get the CockroachDB code:

	```shell
	go get -d github.com/cockroachdb/cockroach
	cd $GOPATH/src/github.com/cockroachdb/cockroach
	```

3.  Run `make build`, `make test`, or anything else our Makefile offers. Note
that the first time you run `make`, it can take some time to download and
install various dependencies. After running `make build`, the `cockroach`
executable will be in your current directory and can be run as shown in the
[README](README.md).

### Other Considerations

- The default binary contains core open-source functionally covered by the
  Apache License 2 (APL2) and enterprise functionality covered by the
  CockroachDB Community License (CCL). To build a pure open-source (APL2)
  version excluding enterprise functionality, use `make buildoss`. See this
  [blog post] for more details.

  [blog post]: https://www.cockroachlabs.com/blog/how-were-building-a-business-to-last/

- If you edit a `.proto` or `.ts` file, you will need to manually regenerate
  the associated `.pb.{go,cc,h}` or `.js` files using `make generate`.

- We advise to run `make generate` using our embedded Docker setup.
  `build/builder.sh` is a wrapper script designed to make this convenient. You
  can run `build/builder.sh make generate` from the repository root to get the
  intended result.

- If you plan on working on the UI, check out [the UI README](pkg/ui).

- To add or update a Go dependency:
  - See [`build/README.md`](build/README.md) for details on adding or updating
    dependencies.
  - Run `make generate` to update generated files.
  - Create a PR with all the changes.

## Style Guide

[Style Guide](STYLE.md)

## Code Review Workflow

+ All contributors need to sign the [Contributor License Agreement](https://cla-assistant.io/cockroachdb/cockroach).

+ Create a local feature branch to do work on, ideally on one thing at a time.
  If you are working on your own fork, see [this tip](http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be correct.

  `git checkout -b update-readme`

+ Hack away and commit your changes locally using `git add` and `git commit`.
  Remember to write tests! The following are helpful for running specific
  subsets of tests:

  ```shell
  make test
  # Run all tests in ./pkg/storage
  make test PKG=./pkg/storage
  # Run all kv tests matching '^TestFoo' with a timeout of 10s
  make test PKG=./pkg/kv TESTS='^TestFoo' TESTTIMEOUT=10s
  # Run the sql logic tests
  make test PKG=./pkg/sql TESTS='TestLogic$$'
  # or, using a shortcut,
  make testlogic
  # Run a specific sql logic subtest
  make test PKG=./pkg/sql TESTS='TestLogic$$/select$$'
  # or, using a shortcut,
  make testlogic FILES=select
  ```

  Logs are disabled during tests by default. To enable them, include
  `TESTFLAGS="-v -show-logs"` as an argument the test command:

  ```shell
  make test ... TESTFLAGS="-v -show-logs"
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

+ Run the test suite locally:

  `make generate check test testrace`

+ When you’re ready for review, groom your work: each commit should pass tests
  and contain a substantial (but not overwhelming) unit of work. You may also
  want to `git fetch origin` and run
  `git rebase -i --exec "make check test" origin/master` to make sure you're
  submitting your changes on top of the newest version of our code. Next, push
  to your fork:

  `git push -u <yourfork> update-readme`

+ Then [create a pull request using GitHub’s UI](https://help.github.com/articles/creating-a-pull-request). If you know of
  another GitHub user particularly suited to reviewing your pull request, be
  sure to mention them in the pull request body. If you possess the necessary
  GitHub privileges, please also [assign them to the pull request using
  GitHub's UI](https://help.github.com/articles/assigning-issues-and-pull-requests-to-other-github-users/).
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

## Debugging

Peeking into a running cluster can be done in several ways:

* the [net/trace](https://godoc.org/golang.org/x/net/trace) endpoint at
  `/debug/requests`.  It has a breakdown of the recent traced requests, in
  particularly slow ones. Two families are traced: `node` and `coord`, the
  former (and likely more interesting one) containing what happens inside of
  `Node`/`Store`/`Replica` and the other inside of the coordinator
  (`TxnCoordSender`).
* [pprof](https://golang.org/pkg/net/http/pprof/) gives us (among other things)
  heap and cpu profiles; [this golang blog post](http://blog.golang.org/profiling-go-programs) explains it extremely well and
  [this one by Dmitry Vuykov](https://software.intel.com/en-us/blogs/2014/05/10/debugging-performance-issues-in-go-programs)
  goes into even more detail.

An easy way to locally run a workload against a cluster are the acceptance
tests. For example,

```shell
make acceptance TESTS='TestPut$$' TESTFLAGS='-v -d 1200s -l .' TESTTIMEOUT=1210s
```

runs the `Put` acceptance test for 20 minutes with logging (useful to look at
the stack trace in case of a node dying). When it starts, all the relevant
commands for `pprof`, `trace` and logs are logged to allow for convenient
inspection of the cluster.
