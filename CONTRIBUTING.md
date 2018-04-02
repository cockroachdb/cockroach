# Contributing to Cockroach

- [Prerequisites](#prerequisites)
- [Getting and Building](#getting-and-building)
- [Style Guide](#style-guide)
- [Commit Messages](#commit-messages)
- [Code Review Workflow](#code-review-workflow)
- [Debugging](#debugging)

## Prerequisites

Before you start contributing, review these [basic
guidelines](https://www.cockroachlabs.com/docs/stable/contribute-to-cockroachdb.html)
on finding a project, determining its complexity, and learning what to
expect in your collaboration with the Cockroach Labs team.

If you *really* want to dig deep into our processes and mindset, you may also
want to peruse our extensive [first PR guide], which is part of our on-boarding for
new engineers.

## Getting and Building

1. Install the following prerequisites, as necessary:

   - Either a working Docker install able to run GNU/Linux binaries
     (e.g. Docker on Linux, macOS, Windows), so you can reuse our
     pre-populated Docker image with all necessary development
     dependencies; or

   - The tools needed to build CockroachDB from scratch:

     - A C++ compiler that supports C++11. Note that GCC prior to 6.0 doesn't
       work due to https://gcc.gnu.org/bugzilla/show_bug.cgi?id=48891
     - The standard C/C++ development headers on your system.
     - On GNU/Linux, the terminfo development libraries, which may be
       part of a ncurses development package (e.g. `libtinfo-dev` on
       Debian/Ubuntu, but `ncurses-devel` on CentOS).
     - A Go environment with a recent 64-bit version of the toolchain. Note that
       the Makefile enforces the specific version required, as it is updated
       frequently.
     - Git 1.9+
     - Bash (4+ is preferred)
     - GNU Make (3.81+ is known to work)
     - CMake 3.1+
     - Autoconf 2.68+
     - NodeJS 6.x and Yarn 1.0+

   Note that at least 4GB of RAM is required to build from source and run tests.

2. Get the CockroachDB code:

   ```shell
   go get -d github.com/cockroachdb/cockroach
   cd $(go env GOPATH)/src/github.com/cockroachdb/cockroach
   ```

3. Run `make build`, `make test`, or anything else our Makefile offers.

If you wish to reuse our builder image instead of installing all the
dependencies manually, prefix the `make` command with
`build/builder.sh`; for example `build/builder.sh make build`.

Note that the first time you run `make`, it can take some time to
download and install various dependencies. After running `make build`,
the `cockroach` executable will be in your current directory and can
be run as shown in the [README](README.md).

### Other Considerations

- The default binary contains core open-source functionally covered by
  the Apache License 2 (APL2) and enterprise functionality covered by
  the CockroachDB Community License (CCL). To build a pure open-source
  (APL2) version excluding enterprise functionality, use `make
  buildoss`. See this [blog post] for more details.

  [blog post]: https://www.cockroachlabs.com/blog/how-were-building-a-business-to-last/

- If you plan on working on the UI, check out [the UI README](pkg/ui).

- To add or update a Go dependency:
  - See [`build/README.md`](build/README.md) for details on adding or updating
    dependencies.
  - Run `make generate` to update generated files.
  - Create a PR with all the changes.

## Style Guide

See our separate [style guide](docs/style.md) document.

## Commit Messages

When you're ready to commit, be sure to write a Good Commit Message™.

Our commit message guidelines are detailed here:
https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages

In summary (the wiki page details the rationales and provides further suggestions):
- Keep in mind who reads: think of the reviewer, think of the release notes
- [Separate subject from body with a blank line](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-title)
- [Use the body to explain *what* and *why* vs. *how*](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-description)
- [Prefix the subject line with the affected package/area](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-title)
- [Include a release note annotation](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#release-notes), in the right position
- [Use the imperative mood in the subject line](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-title)
- [Keep the commit title concise but information-rich](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-title)
- [Wrap the body at some consistent width under 100 characters](https://github.com/cockroachdb/cockroach/wiki/Git-Commit-Messages#commit-description)

## Code Review Workflow

- All contributors need to sign the [Contributor License
  Agreement](https://cla-assistant.io/cockroachdb/cockroach).

- Create a local feature branch to do work on, ideally on one thing at
  a time.  If you are working on your own fork, see [this
  tip](http://blog.campoy.cat/2014/03/github-and-go-forking-pull-requests-and.html)
  on forking in Go, which ensures that Go import paths will be
  correct.

  ```shell
  git checkout -b update-readme
  ```

- Hack away and commit your changes locally using `git add` and `git commit`.
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

- Run the linters, code generators, and unit test suites locally:

  ```shell
  make pre-push
  ````

  This will take several minutes.

- When you’re ready for review, groom your work: each commit should pass tests
  and contain a substantial (but not overwhelming) unit of work. You may also
  want to `git fetch origin` and run
  `git rebase -i --exec "make lint test" origin/master` to make sure you're
  submitting your changes on top of the newest version of our code. Next, push
  to your fork:

  ```shell
  git push -u <yourfork> update-readme
  ```

- Then [create a pull request using GitHub’s
  UI](https://help.github.com/articles/creating-a-pull-request). If
  you know of another GitHub user particularly suited to reviewing
  your pull request, be sure to mention them in the pull request
  body. If you possess the necessary GitHub privileges, please also
  [assign them to the pull request using GitHub's
  UI](https://help.github.com/articles/assigning-issues-and-pull-requests-to-other-github-users/).
  This will help focus and expedite the code review process.

- Address test failures and feedback by amending your commits. If your
  change contains multiple commits, address each piece of feedback by
  amending that commit to which the particular feedback is aimed. Wait
  (or ask) for new feedback on those commits if they are not
  straightforward. An `LGTM` ("looks good to me") by someone qualified
  is usually posted when you're free to go ahead and merge. Most new
  contributors aren't allowed to merge themselves; in that case, we'll
  do it for you.

- Direct merges using GitHub's "big green button" are avoided.  Instead, we use
  [bors-ng](https://bors.tech/documentation/) to manage our merges to prevent
  "merge skew".  When you're ready to merge, add a comment to your PR of the
  form `bors r+`. Craig (our Bors bot)
  will run CI on your changes, and if it passes, merge them.  For more
  information, see [the wiki](https://github.com/cockroachdb/cockroach/wiki/Bors-merge-bot).

## Debugging

Peeking into a running cluster can be done in several ways:

- the [net/trace](https://godoc.org/golang.org/x/net/trace) endpoint
  at `/debug/requests`.  It has a breakdown of the recent traced
  requests, in particularly slow ones. Two families are traced: `node`
  and `coord`, the former (and likely more interesting one) containing
  what happens inside of `Node`/`Store`/`Replica` and the other inside
  of the coordinator (`TxnCoordSender`).
- [pprof](https://golang.org/pkg/net/http/pprof/) gives us (among
  other things) heap and cpu profiles; [this wiki page](https://github.com/cockroachdb/cockroach/wiki/pprof)
  gives an overview and walks you through using it to profile Cockroach.
  [This golang blog post](http://blog.golang.org/profiling-go-programs)
  explains it extremely well and [this one by Dmitry
  Vuykov](https://software.intel.com/en-us/blogs/2014/05/10/debugging-performance-issues-in-go-programs)
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

[first PR guide]: docs/first-pr.md
