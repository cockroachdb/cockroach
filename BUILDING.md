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

The `build/packer/` directory contains what is needed for building VM images for the TeamCity
agents.

The `build/node-run.sh` script is a hack that pipes NodeJS's stdout and stderr through `cat` which
prevents the actual stdout and stderr streams from being infected with non-blocking I/O. The
comments in the script fully explain the scenario. It's used in the Makefile for each node-related
command.

The `build/upload-coverage.sh` script generates code coverage reports of all packages in the `pkg`
directory and uploads the coverage profile to codecov.io. It's run via `make upload-coverage`.

The `bin/uptodate` program is built from `pkg/cmd/uptodate` and used to check if c-deps are
up-to-date or need to be rebuilt.

The `build/werror.sh` script promotes warnings produced by a command to fatal errors, like GCC's
`-Werror` flag.

The `bin/execgen` program is built from `pkg/sql/colexec/execgen/cmd/execgen` and used to generate
templated code related to columnarized execution.

A slew of additional binaries deposited in `bin/` can be built using `make`. See [Go binary
dependency file generation](#go-binary-dependency-file-generation) below for the list.


# Tool-Specific Details

## Make

The CockroachDB makefile is large and complex. Here we focus on summarizing and clarifying it.

`make help` gives an overview of available targets, variables and usage examples. We do not
duplicate that information here.

There are many detailed and helpful comments throughout the Makefile. When information from these
comments is included in this document, it is only in summary.

This make file can:

- run on GNU and BSD systems (many commands have flags set by variables)
- be run with different versions of Make (not just GNU make)
- cross-compile cockroach to darwin, linux-gnu, linux-musl, and windows


### Approximate Sections of the Makefile

Functionality is mostly split into relevant sections. These are the approximate line numbers for
each section (they will surely change). Additonal details on each section are included below.

- Initialization: 0 - 374
- C deps: about 375 - 785
- Cockroach: 801 - 965
- Install/start cockroach: 966 - 976
- Cockroach testing: 977 - 1075
- Auxiliary commands: 1076 - 1159
- .buildinfo: 1160 - 1183
- Protobuf configuration: 1184 - 1307
- UI: 1308 - 1414
- SQL parser: 1415 - 1587
- c-deps-fmt: 1588 - 1592
- Cleaning: 1593 - 1638
- Go binary dependency file generation: 1639 - 1732
- Check CLI variables are all valid: 1733 - EOF


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

This section starts with C/C++ dependency-specific configuration, which includes:

1. Distinguishing the host type by setting `host-is-macos` and `host-is-mingw`. It appears that
   the Makefile default is to make it work on Linux and then tweak it for running on Mac OS or
   Windows. Like with exporting `MACOSX_DEPLOYMENT_TARGET` on macos.

1. Configure some basic variables (`TARGET_TRIPLE`, `XCMAKE_SYSTEM_NAME`, `XGOOS`, `XGOARCH`,
   `XCC`, `XCXX`, `EXTRA_XCMAKE_FLAGS`, `EXTRA_XCONFIGURE_FLAGS`)

1. Determine if `is-cross-compile` and if the `target-is-windows`

1. Some make-specific configuration: `cmake-flags`, `configure-flags`, `xcmake-flags`, and
   `xconfigure-flags`. Then setting specific cross-compilation settings for xcmake-flags` and
   `xconfigure-flags`.

1. Set variables for the base c-deps dir in `C_DEPS_DIR` and then each dependency's source dir
   thereunder.

1. Variables for build variants: `use-stdmalloc`, `use-msan`, `ENABLE_LIBROACH_ASSERTIONS`,
   `ENABLE_ROCKSDB_ASSERTIONS`.

1. Set the `BUILD_DIR`, including fixing it for mingw. Then defines the build directories and
   the library archive file location for each c-dep.

1. Set `DYN_LIB_DIR` and `DYN_EXT` (correctly per host type) in support of `LIBGEOS`.

1. Define which c-libs are included in the OSS binary (`C_LIBS_OSS`) and which in the CCL binary
   (`C_LIBS_CCL`).

1. Configure inclusion of Kerberos5 in the CCL binary only on linux-gnu (non-musl) builds. Sets
   additional vars: `KRB_CPPFLAGS`, `KRB_DIR`.

1. Set `native-tag` (`TARGET_TRIPLE` minus the dashes) since Go doesn't permit dashes in build tags.

Next are steps to generate intermediate files.

1. Insert a `zcgo_flags_{native-tag}.go` file in each cockroach package that uses cgo to inject
   include and library search paths for the c-deps. The `CGO_FLAGS_FILES` variable lists those
   files.

1. Generate updated Makefiles for each dependency using `cmake`. To force regenerating the Makefile
   for a dependency when CMake or configure flags change, the corresponding `c-deps/DEP-rebuild`
   file has its counter incremented by one. This change needs to be committed with the CMake /
   configure flag change. This allows for greater caching of the dependency artifacts. See the
   cockroach Makefile and the `c-deps/DEP-rebuild` files for details.

Finally, we get to the targets that build the dependencies. The targets are always run and check
whether the artifacts are out-of-date using the `bin/uptodate` program. If the artifacts are not
up-to-date, the dependency is built.

The dependency section ends with:

1. setting convenient target names for each dependency (easy to remember and use on the the CLI).

1. a target to run the tests for `libroach`.


#### Cockroach Steps

This section starts with cockroach-specific configuration, which includes:

1. Sets TAGS to include `make` and the value of the `native-tag` variable. Export `LC_ALL=C` so all
   targets produce consistent results (due to sort order).

1. Reset `build/defs.mk.sig` if the build signature changed.

1. Define the resulting binary names for `COCKROACH`, `COCKROACHOSS` and `COCKROACHSHORT`

1. Define target lists for `SQLPARSER_TARGETS`, `PROTOBUF_TARGETS`, `DOCGEN_TARGETS`,
  `EXECGEN_TARGETS`, `OPTGEN_TARGETS`, `go-targets-ccl`, `go-targets`. It also defines
  `remove_obsolete_execgen` in there, which removes its namesake files.

Next are (mostly) the targets:

1. The `all` and `c-deps` targets are defined. `c-deps` builds the targets in `C_LIBS_CCL`.

1. Many refinements of environment variable values are made and some dependent targets are defined
   per-target. This makes it so `go-install`, `${COCKROACH)`, `${COCKROACHOSS)`, and
   `${COCKROACHSHORT)` all end up running the same recipe but with different parameters to effect
   their unique outcomes.

    - `build-mode` defines the go command to be run. One of: `build -o TARGET_FILE_NAME`, `install`
    - `BUILDTARGET` for which target source directory to start from (`./pkg/cmd/cockroach`,
      `./pkg/cmd/cockroach-oss`, `./pkg/cmd/cockroach-short`)
    - Adding to `LINKFLAGS` for `$(go-targets)`, `go-install` and the cockroach targets

1. The target and recipe for `go-install`, `${COCKROACH)`, `${COCKROACHOSS)`, and
   `${COCKROACHSHORT)`.

1. Targets for friendly make target names for `build` -> `${COCKROACH)`,
   `buildoss` -> `${COCKROACHOSS)`, and `buildshort` -> `${COCKROACHSHORT)`. These targets also:

   - build documentation for the SQL functions and SQL grammar
   - document the settings used for cross-compipled binaries for `build` and `buildshort`


#### Install / Start Cockroach Steps

There are only two things that happen in this section:

- `install`: a target to build and install the `$(COCKROACH)` binary (default location:
  `/usr/local/bin`)
- `start`: build and then start an instance of the `$(COCKROACH)` binary


#### Cockroach Testing

This section is mostly target and recipe definition for test-related targets. The targets are
listed here and include notes about them here only if they are not included in `make help` or if
there is otherwise something interesting to know about the target.

- `testbuild`
- `check`: equivalent to `make test`
- `test`
- `testshort`
- `testrace`
- `stress`
- `stressrace`
- `roachprod-stress`: stress a test or set of tests by running it/them on a roachprod cluster.
  Similar to the normal stress target but with more parallelism to help reproduce flakes faster (or
  prove a flake is fixed). Invocation is similar to `make stress` with the addition of the CLUSTER
  variable which is used to specify the roachprod cluster to run the tests on. The roachprod cluster
  must already exist.
- `roachprod-stressrace`: same as `roachprod-stress` but with the Go race detector enabled.
- `bench`
- `benchshort`: run one iteration of each benchmark and skip the longer-running benchmarks.
- `testlogic`: runs the logic tests in `./pkg/sql/logictest`.
- `testbaselogic`: runs the logic tests in `./pkg/ccl/logictestccl`.
- `testccllogic`: runs the logic tests in `./pkg/sql/opt/exec/execbuilder`.
- `testoptlogic`
- `testslow`: tell which tests are slow tests.
- `testraceslow`: tell which tests are slow when run with the Go race detector enabled.
- `upload-coverage`: calls the `build/upload-coverage.sh` script.
- `acceptance`: Runs a shell script that calls `make test` to run only the slower-to-run
  acceptance tests (marked as such with the `acceptance` tag).
- `compose`: Runs a shell script that calls `make test` to run only the tests needing
  docker-compose.


#### Auxiliary Commands

(Note: There may be a better title for this section.)

This section is mostly target and recipe definition for test-related targets. The targets are
listed here and include notes about them here only if they are not included in `make help` or if
there is otherwise something interesting to know about the target.

- `dupl`: Find duplicate code in the Go source code. Uses https://github.com/mibk/dupl .
- `generate`
- `lint`: runs all style checkers and linters. It does not run the URL checker (`bin/urlcheck`).
- `lintshort`
- `protobuf`
- `pre-push`: Intended for running before pushing your changes up. It runs many of the checks CI
  runs: `generate`, `lint`, `test`, `ui-lint`, `ui-test`.
- `archive`


#### .buildinfo

A short section containing these targets:

- `.buildinfo`: Creates the directory.
- `.buildinfo/tag`: The git tag for the version that would be built.
- `.buildinfo/rev`: The git revision sha.

Both `.buildinfo/tag` and `.buildinfo/rev` are rebuilt with every run of make + reference to the
file(s).


#### Protobuf Configuration and Code Generation

This section contains configuration and targets for generating protobuf code. Supported languages:
Go, C++, JavaScript, TypeScript.

The steps in the section:

1. Define variables detailing the locations of all the `.proto` files.

1. Find all the Go and C++ proto files and generates the required code files. The Go and C++ files
   do not automatically find dependent protobuf entry points so these recipes find all the right
   files. This code is hairy and includes many sed commands. The `build/werror.sh` script is used
   and the generated Go code is formatted.

1. Generate the JavaScript and TypeScript protobuf files. The JavaScript compiler only needs the
   entrypoint protobufs to be listed and will automatically find and compile any protobufs that
   are depended upon from there.


#### UI

1. Define variables for locations of programs used in the UI recipes

1. Define a few targets: `ui-generate`, `ui-fonts`, `ui-topo`, `ui-lint`. They are all pretty
   straight forward. `ui-generate` is simple because it depends on `pkg/ui/distccl/bindata.go`,
   which does the heavy lifting.

1. Define a few more variables for webpack bundles and manifests followed by rules to correctly
   build them (webpack outputs two files with one command while the Make distributed by Apple would
   run webpack twice, once for each file; these rules get Make to only run weback once).

1. Define many targets:

   - `ui-test`: Run the UI tests.
   - `ui-test-watch`: Run the UI tests then watch for file changes and automatically rerun the tests.
   - `ui-test-debug`: Like `ui-test-watch` but in debug mode using Chrome.
   - `pkg/ui/dist*/bindata.go`: This is where the meat of building the UI code and putting it in a
     go-includable/consumable format happens.
   - `pkg/ui/yarn.op.installed`: Ensure yarn is installed. Used in many of the other recipes.
   - `ui-watch`: Spin up a development webpack server in watch mode so it automatically recompiles
     when files change.
   - `ui-watch-secure`: A secure HTTPS version of `ui-watch`.
   - `ui-clean`
   - `ui-maintainer-clean`


#### SQL Parser

The first section deals with generating the SQL parser. It has many informational comments and
these targets:

- `pkg/sql/parser/gen/sql.go.tmp`: Generate the SQL parser code using goyacc.
- `pkg/sql/lex/tokens.go`: Move the SQL parser tokens to their own package.
- `pkg/sql/parser/sql.go`: Modify the SQL parser to reference the `lex` package for the tokens.
- `pkg/sql/parser/gen/sql-gen.y`: Modify the generated SQL parser with a couple long command
  pipelines.
- `pkg/sql/lex/reserved_keywords.go`: Generate the list of reserved keywords in the lex package.
- `pkg/sql/lex/keywords.go`: Generate the list of all keywords in the lex packages.
- `sqlparser-unused-unreserved-keywords`: Print unreserved keywords that aren't used to the build
  logs.
- `pkg/sql/parser/helpmap_test.go`: Generate help text for use in tests using
  `pkg/sql/parser/help_gen_test.sh`.
- `pkg/sql/parser/help_messages.go`: Generate an easy-to-reference map of help messages in the
  `parser` package from the generated SQL parser using the `pkg/sql/parser/help.awk` script.

The next section generates documentation files:

- `bin/.docgen_bnfs`: Generate docs for each SQL command in BNF format.
- `bin/.docgen_functions`: Generate .md files listing the aggregates, functions, operators and
  window functions.
- `$(SETTINGS_DOC_PAGE)`: Generate an HTML page listing the cockroach cluster settings.

The next section generates templated code related to columnarized execution.

- `bin/execgen_out.d`: Generate a dependency file for including in the Makefile a couple lines
  later. It matches the `*.eg.go` files to the right templates they depend upon.
- `%.eg.go`: Generate the colexec files using `bin/execgen`.

The last section generates cost-gased optimizer code.

- Set a couple variables to use as dependencies in the following rules: `optgen-defs`,
  `optgen-norm-rules`, and `optgen-xform-rules`.
- Rules to generate the optimizer code: `pkg/sql/opt/memo/expr.og.go`, `pkg/sql/opt/operator.og.go`,
  `pkg/sql/opt/rule_name.og.go`, `pkg/sql/opt/rule_name_string.go`,
  `pkg/sql/opt/xform/explorer.og.go`, `pkg/sql/opt/norm/factory.og.go`.


#### c-deps-fmt

A single rule, `c-deps-fmt`, that formats non-generated .cc and .h files in libroach using
clang-format.


#### Cleaning

Rules for cleaning up. There's three levels:

- `clean`: removes only build artifiacts not including UI-related files.
- `maintainer-clean`: `clean` plus some auto-generated source code and UI-related files.
- `unsafe-clean`: `maintainer-clean` plus ALL untracked/ignored files.


#### Go binary dependency file generation

There are at least 36 binaries besides `cockroach` that live in the cockroach repository.
Dependencies for these binaries are auto-generated using the approach [detailed
here](http://make.mad-scientist.net/papers/advanced-auto-dependency-generation/). Here we discuss
a summary of how it works.

This section lists all the binaries in variables, notes where their packages live in the repo (most
are under `pkg/cmd`), adds some additional dependencies for binaries that depend on generated code,
and then has two static pattern rules that match all the binaries:

- `$(bins)`: Build these binaries.
- `$(testbins)`: Use `go test` to build and then test these binaries.

There are also these rules:

- `bin/prereqs`: This program is used to generate the dependency lists for all the other binaries,
  thus it must be built before any of the other binaries and needs to be defined separately.
- `fuzz`: To run fuzz tests using `bin/fuzz`.
- `bin/%.d`: Dependency definition for all the `bin/%` binaries. It is accompanied by an `include`
  line for the files. These files are not included for help or cleanup.

Here is the list of binaries. Those who's package location is not in `pkg/cmd` have their package
location noted.

Regular binaries:

- `bin/allocsim`
- `bin/benchmark`
- `bin/cockroach-oss`
- `bin/cockroach-short`
- `bin/docgen`
- `bin/execgen`: `./pkg/sql/colexec/execgen/cmd/execgen`
- `bin/fuzz`
- `bin/generate-binary`
- `bin/github-post`
- `bin/github-pull-request-make`
- `bin/gossipsim`
- `bin/langgen`: `./pkg/sql/opt/optgen/cmd/langgen`
- `bin/optfmt`: `./pkg/sql/opt/optgen/cmd/optfmt`
- `bin/optgen`: `./pkg/sql/opt/optgen/cmd/optgen`
- `bin/protoc-gen-gogoroach`
- `bin/publish-artifacts`
- `bin/publish-provisional-artifacts`
- `bin/returncheck`
- `bin/roachprod`
- `bin/roachprod-stress`
- `bin/roachtest`
- `bin/roachvet`
- `bin/teamcity-trigger`
- `bin/terraformgen`: `./pkg/cmd/roachprod/vm/aws/terraformgen`
- `bin/uptodate`
- `bin/urlcheck`
- `bin/workload`
- `bin/zerosum`

Testing binaries:

- `bin/logictest`: `./pkg/sql/logictest`
- `bin/logictestccl`: `./pkg/ccl/logictestccl`
- `bin/logictestopt`: `./pkg/sql/opt/exec/execbuilder`


#### Check CLI variables are all valid

The final set of rules are for checking that all variables specified on the command line are valid
variables for use in the Makefile. First, the `build/variables.mk` file is generated, then it's
included and finally all the variables from the CLI are checked that they are in the valid set.


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

TODO: find and document where these scripts are used:

- *teamcity-bless-provisional-binaries.sh*
- *teamcity-publish-provisional-binaries.sh*
- *teamcity-rebuild-agent.sh*
- *teamcity-reset-nightlies.sh*
- *teamcity-stress.sh*
- *teamcity-test-deadlock.sh*
- *teamcity-testlogicrace.sh*
- *teamcity-trigger.sh*
- *teamcity-urlcheck.sh*


