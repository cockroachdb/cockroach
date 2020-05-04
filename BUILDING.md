TODO:

- document all the build tooling we have in the cockroach repo
-- make
-- DONE: build/builder.sh
-- PARTIAL: build/teamcity-*
-- DONE: build/builder/mkrelease.sh
-- how bin/*.d dependency files are generated. What commands they generate / how to figure that out.
-- ...
- Figure out where the rest of the teamcity-* scripts are used. Document the teamcity build configs in the wiki doc.


To answer:

- what binaries can be created?
-- cockroach, roachtest, roachprod, workload, gceworker?, ...
- what docs already exist?









# Overview

The cockroach repository contains many tools used during the development, build, CI and release
process. This document describes how they are used and their context to each other.


## The Tools

`make` is where it all begins. With `make`, we build the cockroach binary, run unit tests, lint,
build auxilary tools (roachprod, roachtest, workload, ...), etc.

`build/builder.sh` lets us run `make` and other other tools within docker containers. Additionaly,
it provides the ability to build and manage docker images.

`build/builder/mkrelease.sh` is used to build release binaries of CockroachDB, including
cross-compiling for different platforms. It must be run inside the `cockroachdb/builder` docker
container (usually via `build/builder.sh`).

The `build/teamcity-*.sh` scripts are how most TeamCity build configurations interact with the
`cockroach` repository. These scripts use `build/builder.sh` to run `make` and
`build/builder/mkrelease.sh` on TeamCity agents.

The `build/node-run.sh` script is a hack that pipes NodeJS's stdout and stderr through `cat` which
prevents the actual stdout and stderr streams from being infected with non-blocking I/O. The
comments in the script fully explain the scenario. It's used in the Makefile for each node-related
command.

The `build/upload-coverage.sh` script generates code coverage reports of all packages in the `pkg`
directory and uploads the coverage profile to codecov.io. It's run via `make upload-coverage`.


# Tool-Specific Details

## Make

The CockroachDB makefile is large and complex. Here we focus on summarizing and clarifying it.

`make help` gives an overview of available targets, variables and usage examples. We do not
duplicate that information here.

There are many detailed and helpful comments throughout the Makefile. When information from these
comments is included in this document, it is only in summary.


Some general notes: (TODO: better integrate this into the make overview flow)

- Vendored tools are installed in the `bin` directory within the repo. See the `GO_INSTALL` variable.
- A lot of effort has been made to make the script:

    - run on GNU and BSD systems. Many commands have flags set by variables
    - with different versions of Make
    - to cross-compile cockroach
    - each of the included libs is spelled out separately in the file


### Approximate Sections of the Makefile

Functionality is mostly split into relevant sections. These are the approximate line numbers for
each section. Additonal details on each section are included below.

- Initialization: 0 - about 408
- C deps: about 409 - 785
- Cockroach: 801 - 965
- install/start cockroach: 966 - 976
- Cockroach testing (test bench stress roachprod-stress testlogic \*race \*slow upload-coverage acceptance compose): 977 - 1080
- Auxiliary commands (dupl generate lint\* protobuf pre-push archive): 1081 - 1159
- .buildinfo: 1160 - 1183
- Protobuf configuration: 1184 - 1307
- UI (build test clean): 1308 - 1414
- SQL parser (generation help docs execgen optgen): 1415 - 1587
- c-deps-fmt: 1588 - 1592
- Cleaning: 1593 - 1638
- Go binary dependency file generation: 1639 - 1710
- Fuzz (run fuzz tests): 1711 - 1715
- Final things (don't include dependency files for help/cleanup, build/variables.mk): 1717 - EOF


#### Initialization Steps

1. The `GO` and `xgo` (needed for `defs.mk`) variables are set.

1. Some variables need to be computed using shell commands. The first step in the make file is to compute and cache a set of them in the `build/defs.mk` file so they don't get recomputed on every run of make. `build/defs.mk` is then included in `Makefile`.

1. Many targets require the vendor modules to be set up. The second step that is performed is to set up the git submodules using the `bin/.submodules-initialized` target. The file is empty and only serves as a flag. The target is usually referenced as an order-only-prerequisite dependency (i.e., after the `|` on a target definition line).

1. Sometimes you may want to set a variable the same for all runs of make. Add the variable(s) to the `customenv.mk` file and it will be used for every run of `make`. Format: `VAR_NAME = VALUE`.

1. A check is performed to ensure `BENCHES` is only used with the `bench` goal and `TESTS` is not used with the `bench` goal.

1. A check is performed to prevent invoking make with a specific test / bench name without specifying a constraining package.

1. A check to ensure `TYPE` is not set with instructions to use `build/builder.sh mkrelease` instead.

1. Building with dependency files (the `build-with-dep-files` variable) is set for all goals except `help` and `clean`.

1. A large set of variables are defined, many with documentation, some with default values.

    - documented: `PKG`, `TESTS`, `BENCHES`, `FILES`, `TESTCONFIG`, `SUBTESTS`, `LINTTIMEOUT`, `TESTTIMEOUT`, `RACETIMEOUT`, `ACCEPTANCETIMEOUT`, `BENCHTIMEOUT`, `TESTFLAGS`, `GOTESTFLAGS`, `STRESSFLAGS`, `CLUSTER`, `VERBOSE`, `DESTDIR`
    - undocumented: `DUPLFLAGS`, `GOFLAGS`, `TAGS`, `ARCHIVE`, `STARTFLAGS`, `BUILDTARGET`, `SUFFIX`, `INSTALL`, `prefix`, `bindir`

1. If the make command line contains the `-j` flag and NCPUS is set, then `-j${NCPUS}` is added to MAKEFLAGS.

1. The `help` target is defined

1. Many more variables are defined:

    - `BUILDTYPE`: options are `development` and `release`
    - Build C/C++ with basic debugging info: `CFLAGS`, `CXXFLAGS`, `LDFLAGS`, `CGO_CFLAGS`, `CGO_CXXFLAGS`, `CGO_LDFLAGS`
    - `LINKFLAGS`: used instead of LDFLAGS because LDFLAGS has built-in semantics that don't make sense with the Go toolchain
    - `GOFLAGS`, `TAR`
    - `GOPATH`: Ensure it has one, unambiguous entry.
    - `GO_INSTALL`: command to install vendored tools in a directory within the repository

1. `export PATH` to prefer our installed tools.

1. `export SHELL` as a hack to make `make` use the newly exported `PATH` and force the `PWD` env var to `$(CURDIR)`.

1. Many more variables defined:

    - `make-lazy`: convert a recursive variable to a lazy variable.
    - `TAR_XFORM_FLAG`: Get the right GNU / BSD tar flags for `xform`.
    - `SED_INPLACE`: Get the right GNU / BSD flag for `sed` applying changes in-place to a file.
    - `MAKE_TERMERR`
    - `space`: to get a literal space in a Makefile
    - Color support: `yellow`, `cyan`, `term-reset`

1. `.DELETE_ON_ERROR` is set

1. Set the target `.ALWAYS_REBUILD` for use in targets that name a real file that must be rebuilt on every Make invocation.

1. Set the `GITHOOKS` variable and define the `$(GITHOOKSDIR)/%` target. Only if in a git worktree.


#### C Dependency Steps



#### Cockroach Steps

Questions:
- what's the diff between cockroach, cockroachshort and cockroachoss?





## build/builder.sh

This script manages the docker image in the docker hub repository `cockroachdb/builder`. It is
hard-coded with (usually) the latest version of the docker image specified in the `version`
variable, which is the tag on the image.

`build/builder.sh pull` pulls the hard-coded version down to the local computer.

`build/builder.sh init` builds / rebuilds the docker image using the dockerfile at
`build/builder/Dockerfile`.

`build/builder.sh push` calls `init` to build / rebuild the image, tags the image with the current
date and time (after init is completed) and pushes the image to docker hub. To make the new image
used everywhere, follow the [directions in the
`build/README.md`](https://github.com/cockroachdb/cockroach/blob/master/build/README.md#upgrading--extending-the-docker-image)
file.

`build/builder.sh version` prints the tag of the hard-coded image version.

`build/builder.sh COMMAND` runs `COMMAND` in a container of the hard-coded version. Some details:

- Artifacts are saved in the `COCKROACH_REPOSITORY_ROOT/artifacts` directory.
- The builder home directory in the container is mapped to the `build/builder_home` directory on
  the host so data can persist across runs (like yarn cache and the go build cache).
- It has built-in settings for running on linux and OS X.
- If running in TeamCity, it mounts a volume for the full git repository alternates path.
- Ensures these directories exist in the COCKROACH_REPOSITORY_ROOT: `bin`, `bin.docker_amd64`,
  `lib`, `lib.docker_amd64`. Mounts the `*.docker_amd64` directories onto their bin / lib
  counterparts in the container.
- Ensures `docker/bin`, `docker/native` and `docker/pkg` exist in the hosts `gocache` directory
  and then mount them to `/go/{bin,native,pkg}` respectively.
- If the command fails during running, it checks that there is enough RAM given to the container
  and tells you if there isn't enough.


## build/builder/mkrelease.sh

The [script itself](./build/builder/mkrelease.sh) has good usage instructions in its introductory comments. Some key things to note:

- Usage:  mkrelease [CONFIGURATION] [MAKE-GOALS...]
- It calls `make` with the specified MAKE-GOALS plus architecture-specific configuration parameters according to the specified architecture in CONFIGURATION.

A couple example commands (try `grep -r mkrelease` in your cockroach checkout for others):

- Build an official binary targeting linux musl:

    ```
    mkrelease linux-musl GOFLAGS= SUFFIX=.linux-2.6.32-musl-amd64 TAGS= BUILDCHANNEL=official-binary
    ```

- Build the docker compose checking tests:

    ```
    mkrelease linux-gnu -Otarget testbuild PKG=./pkg/compose/compare/compare TAGS=compose
    ```


## `build/teamcity-*.sh` files

Many of the TeamCity build configurations run one or more `build/teamcity-*` scripts to do their work. What they each do is documented in [Cockroach DB: Source to Binary](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary) in the context of where they are used in TeamCity. Here is where each of them are used:

- *teamcity-acceptance.sh*:   [acceptance](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#acceptance)
- *teamcity-assert-clean.sh*:   [Compose Tests](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Compose-Tests) and [Go Test Template](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Go-Test-Template)
- *teamcity-bench.sh*:   [bench](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#bench)
- *teamcity-build-test-binary.sh*:   [Build Test Binary](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Build-Test-Binary)
- *teamcity-check.sh*:   [lint](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#lint)
- *teamcity-compose.sh*:   [Compose Tests](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Compose-Tests)
- *teamcity-local-roachtest.sh*:   [roachtest](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#roachtest)
- *teamcity-nightly-roachtest-invoke.sh*:   [Roachtest Nightly - GCE](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Roachtest-Nightly---GCE)
- *teamcity-nightly-roachtest.sh*:   [Roachtest Nightly - GCE](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Roachtest-Nightly---GCE)
- *teamcity-publish-artifacts.sh*:   [Publish Bleeding Edge](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Publish-Bleeding-Edge)
- *teamcity-publish-s3-binaries.sh*:   [Publish Bleeding Edge](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#Publish-Bleeding-Edge)
- *teamcity-sqllogictest.sh*:   [SQLite Logic Tests](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#SQLite-Logic-Tests)
- *teamcity-test.sh*:   [test](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#test)
- *teamcity-testlogic-verbose.sh*:   [SQLite Logit Tests High VModule Nightly](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#SQLite-Logic-Tests-High-VModule-Nightly)
- *teamcity-testrace.sh*:   [testrace](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#testrace)
- *teamcity-verify-archive.sh*:   [verify-archive](https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/412254816/Cockroach+DB+Source+to+Binary#verify-archive)

The `teamcity-support.sh` script is used in many of the other teamcity-* scripts.

Need to find and document where these scripts are used:

- *teamcity-bless-provisional-binaries.sh*
- *teamcity-publish-provisional-binaries.sh*
- *teamcity-rebuild-agent.sh*
- *teamcity-reset-nightlies.sh*
- *teamcity-stress.sh*
- *teamcity-test-deadlock.sh*
- *teamcity-testlogicrace.sh*
- *teamcity-trigger.sh*
- *teamcity-urlcheck.sh*


