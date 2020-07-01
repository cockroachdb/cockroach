# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# WARNING: This Makefile is not easily understood. If you're here looking for
# typical Make invocations to build the project and run tests, you'll be better
# served by running `make help`.
#
# Maintainers: the output of `make help` is automatically generated from the
# double-hash (##) comments throughout this Makefile. Please submit
# improvements!

# We need to define $(GO) early because it's needed for defs.mk.
GO      ?= go
# xgo is needed also for defs.mk.
override xgo := GOFLAGS= $(GO)

# defs.mk stores cached values of shell commands to avoid recomputing them on
# every Make invocation. This has a small but noticeable effect, especially on
# noop builds.
# This needs to be the first rule because we're including build/defs.mk
# first thing below, and Make needs to know how to build it.
.SECONDARY: build/defs.mk
build/defs.mk: Makefile build/defs.mk.sig
ifndef IGNORE_GOVERS
	@build/go-version-check.sh $(GO) || { echo "Disable this check with IGNORE_GOVERS=1." >&2; exit 1; }
endif
	@echo "macos-version = $$(sw_vers -productVersion 2>/dev/null | grep -oE '[0-9]+\.[0-9]+')" > $@.tmp
	@echo "GOEXE = $$($(xgo) env GOEXE)" >> $@.tmp
	@echo "NCPUS = $$({ getconf _NPROCESSORS_ONLN || sysctl -n hw.ncpu || nproc; } 2>/dev/null)" >> $@.tmp
	@echo "UNAME = $$(uname)" >> $@.tmp
	@echo "HOST_TRIPLE = $$($$($(GO) env CC) -dumpmachine)" >> $@.tmp
	@echo "GIT_DIR = $$(git rev-parse --git-dir 2>/dev/null)" >> $@.tmp
	@echo "GITHOOKSDIR = $$(test -d .git && echo '.git/hooks' || git rev-parse --git-path hooks)" >> $@.tmp
	@echo "have-defs = 1" >> $@.tmp
	@set -e; \
	if ! cmp -s $@.tmp $@; then \
	   mv -f $@.tmp $@; \
	   echo "Detected change in build system. Rebooting make." >&2; \
	else rm -f $@.tmp; fi

include build/defs.mk

# Nearly everything below this point needs to have the vendor directory ready
# for use and will depend on bin/.submodules-initialized. In order to
# ensure this is available before the first "include" directive depending
# on it, we'll have it listed first thing.
#
# Note how the actions for this rule are *not* using $(GIT_DIR) which
# is otherwise defined in defs.mk above. This is because submodules
# are used in the process of definining the .mk files included by the
# Makefile, so it is not yet defined by the time
# `.submodules-initialized` is needed during a fresh build after a
# checkout.
.SECONDARY: bin/.submodules-initialized
bin/.submodules-initialized:
	gitdir=$$(git rev-parse --git-dir 2>/dev/null || true); \
	if test -n "$$gitdir"; then \
	   git submodule update --init --recursive; \
	fi
	mkdir -p $(@D)
	touch $@

# If the user wants to persist customizations for some variables, they
# can do so by defining `customenv.mk` in their work tree.
-include customenv.mk

ifeq "$(findstring bench,$(MAKECMDGOALS))" "bench"
$(if $(TESTS),$(error TESTS cannot be specified with `make bench` (did you mean BENCHES?)))
else
$(if $(BENCHES),$(error BENCHES can only be specified with `make bench`))
endif

# Prevent invoking make with a specific test name without a constraining
# package.
ifneq "$(filter bench% test% stress%,$(MAKECMDGOALS))" ""
ifeq "$(PKG)" ""
$(if $(subst -,,$(TESTS)),$(error TESTS must be specified with PKG (e.g. PKG=./pkg/sql)))
$(if $(subst -,,$(BENCHES)),$(error BENCHES must be specified with PKG (e.g. PKG=./pkg/sql)))
endif
endif

TYPE :=
ifneq "$(TYPE)" ""
$(error Make no longer understands TYPE. Use 'build/builder.sh mkrelease $(subst release-,,$(TYPE))' instead)
endif

# dep-build is set to non-empty if the .d files should be included.
# This definition makes it empty when only the targets "help" and/or "clean"
# are specified.
build-with-dep-files := $(or $(if $(MAKECMDGOALS),,implicit-all),$(filter-out help clean,$(MAKECMDGOALS)))

## Which package to run tests against, e.g. "./pkg/foo".
PKG := ./pkg/...

## Tests to run for use with `make test` or `make check-libroach`.
TESTS := .

## Benchmarks to run for use with `make bench`.
BENCHES :=

## Space delimited list of logic test files to run, for make testlogic/testccllogic/testoptlogic.
FILES :=

## Name of a logic test configuration to run, for make testlogic/testccllogic/testoptlogic.
## (default: all configs. It's not possible yet to specify multiple configs in this way.)
TESTCONFIG :=

## Regex for matching logic test subtests. This is always matched after "FILES"
## if they are provided.
SUBTESTS :=

## Test timeout to use for the linter.
LINTTIMEOUT := 20m

## Test timeout to use for regular tests.
TESTTIMEOUT := 30m

## Test timeout to use for race tests.
RACETIMEOUT := 30m

## Test timeout to use for acceptance tests.
ACCEPTANCETIMEOUT := 30m

## Test timeout to use for benchmarks.
BENCHTIMEOUT := 5m

## Extra flags to pass to the go test runner, e.g. "-v --vmodule=raft=1"
TESTFLAGS :=

## Flags to pass to `go test` invocations that actually run tests, but not
## elsewhere. Used for the -json flag which we'll only want to pass
## selectively.  There's likely a better solution.
GOTESTFLAGS :=

## Extra flags to pass to `stress` during `make stress`.
STRESSFLAGS :=

## Cluster to use for `make roachprod-stress`
CLUSTER :=

## Verbose allows turning on verbose output from the cmake builds.
VERBOSE :=

## Indicate the base root directory where to install
DESTDIR :=

DUPLFLAGS    := -t 100
GOFLAGS      :=
TAGS         :=
ARCHIVE      := cockroach.src.tgz
STARTFLAGS   := -s type=mem,size=1GiB --logtostderr
BUILDTARGET  := ./pkg/cmd/cockroach
SUFFIX       := $(GOEXE)
INSTALL      := install
prefix       := /usr/local
bindir       := $(prefix)/bin

# We always want to build from the vendor directory.
# Avoid reusing GOFLAGS as that is overwritten by various release processes.
GOMODVENDORFLAGS := -mod=vendor

ifeq "$(findstring -j,$(shell ps -o args= $$PPID))" ""
ifdef NCPUS
MAKEFLAGS += -j$(NCPUS)
$(info Running make with -j$(NCPUS))
endif
endif

help: ## Print help for targets with comments.
	@echo "Usage:"
	@echo "  make [target...] [VAR=foo VAR2=bar...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, $$2}'
	@echo ""
	@echo "Useful variables:"
	@awk 'BEGIN { FS = ":=" } /^## /{x = substr($$0, 4); getline; if (NF >= 2) printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, x}' $(MAKEFILE_LIST) | sort
	@echo ""
	@echo "Typical usage:"
	@printf "  $(cyan)%s$(term-reset)\n    %s\n\n" \
		"make test" "Run all unit tests." \
		"make test PKG=./pkg/sql" "Run all unit tests in the ./pkg/sql package" \
		"make test PKG=./pkg/sql/parser TESTS=TestParse" "Run the TestParse test in the ./pkg/sql/parser package." \
		"make bench PKG=./pkg/sql/parser BENCHES=BenchmarkParse" "Run the BenchmarkParse benchmark in the ./pkg/sql/parser package." \
		"make testlogic" "Run all OSS SQL logic tests." \
		"make testccllogic" "Run all CCL SQL logic tests." \
		"make testoptlogic" "Run all opt exec builder SQL logic tests." \
		"make testbaselogic" "Run all the baseSQL SQL logic tests." \
		"make testlogic FILES='prepare|fk'" "Run the logic tests in the files named prepare and fk (the full path is not required)." \
		"make testlogic FILES=fk SUBTESTS='20042|20045'" "Run the logic tests within subtests 20042 and 20045 in the file named fk." \
		"make testlogic TESTCONFIG=local" "Run the logic tests for the cluster configuration 'local'." \
		"make fuzz" "Run all fuzz tests for 12m each (or whatever the default TESTTIMEOUT is)." \
		"make fuzz PKG=./pkg/sql/... TESTTIMEOUT=1m" "Run all fuzz tests under the sql directory for 1m each." \
		"make fuzz PKG=./pkg/sql/sem/tree TESTS=Decimal TESTTIMEOUT=1m" "Run the Decimal fuzz tests in the tree directory for 1m." \
		"make check-libroach TESTS=ccl" "Run the libroach tests matching .*ccl.*"

BUILDTYPE := development

# Build C/C++ with basic debugging information.
CFLAGS += -g1
CXXFLAGS += -g1
LDFLAGS ?=

# TODO(benesch): remove filter-outs below when golang/go#26144 and
# golang/go#16651, respectively, are fixed.
CGO_CFLAGS = $(filter-out -g%,$(CFLAGS))
CGO_CXXFLAGS = $(CXXFLAGS)
CGO_LDFLAGS = $(filter-out -static,$(LDFLAGS))

export CFLAGS CXXFLAGS LDFLAGS CGO_CFLAGS CGO_CXXFLAGS CGO_LDFLAGS

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
override LINKFLAGS = -X github.com/cockroachdb/cockroach/pkg/build.typ=$(BUILDTYPE) -extldflags "$(LDFLAGS)"

GOMODVENDORFLAGS ?= -mod=vendor
GOFLAGS ?=
TAR     ?= tar

# Ensure we have an unambiguous GOPATH.
GOPATH := $(shell $(GO) env GOPATH)

ifneq "$(or $(findstring :,$(GOPATH)),$(findstring ;,$(GOPATH)))" ""
$(error GOPATHs with multiple entries are not supported)
endif

GOPATH := $(realpath $(GOPATH))
ifeq "$(strip $(GOPATH))" ""
$(error GOPATH is not set and could not be automatically determined)
endif

ifeq "$(filter $(GOPATH)%,$(CURDIR))" ""
$(error Current directory "$(CURDIR)" is not within GOPATH "$(GOPATH)")
endif

ifeq "$(GOPATH)" "/"
$(error GOPATH=/ is not supported)
endif

$(info GOPATH set to $(GOPATH))

# We install our vendored tools to a directory within this repository to avoid
# overwriting any user-installed binaries of the same name in the default GOBIN.
GO_INSTALL := GOBIN='$(abspath bin)' GOFLAGS= $(GO) install

# Prefer tools we've installed with go install and Yarn to those elsewhere on
# the PATH.
export PATH := $(abspath bin):$(PATH)

# HACK: Make has a fast path and a slow path for command execution,
# but the fast path uses the PATH variable from when make was started,
# not the one we set on the previous line. In order for the above
# line to have any effect, we must force make to always take the slow path.
# Setting the SHELL variable to a value other than the default (/bin/sh)
# is one way to do this globally.
# http://stackoverflow.com/questions/8941110/how-i-could-add-dir-to-path-in-makefile/13468229#13468229
#
# We also force the PWD environment variable to $(CURDIR), which ensures that
# any programs invoked by Make see a physical CWD without any symlinks. The Go
# toolchain does not support symlinks well (for one example, see
# https://github.com/golang/go/issues/24359). This may be fixed when GOPATH is
# deprecated, so revisit whether this workaround is necessary then.
export SHELL := env PWD=$(CURDIR) bash
ifeq ($(SHELL),)
$(error bash is required)
endif

# Invocation of any NodeJS script should be prefixed by NODE_RUN. See the
# comments within node-run.sh for rationale.
NODE_RUN := build/node-run.sh

# make-lazy converts a recursive variable, which is evaluated every time it's
# referenced, to a lazy variable, which is evaluated only the first time it's
# used. See: http://blog.jgc.org/2016/07/lazy-gnu-make-variables.html
override make-lazy = $(eval $1 = $$(eval $1 := $(value $1))$$($1))

# GNU tar and BSD tar both support transforming filenames according to a regular
# expression, but have different flags to do so.
TAR_XFORM_FLAG = $(shell $(TAR) --version | grep -q GNU && echo "--xform='flags=r;s'" || echo "-s")
$(call make-lazy,TAR_XFORM_FLAG)

# To edit in-place without creating a backup file, GNU sed requires a bare -i,
# while BSD sed requires an empty string as the following argument.
SED_INPLACE = sed $(shell sed --version 2>&1 | grep -q GNU && echo -i || echo "-i ''")
$(call make-lazy,SED_INPLACE)

# MAKE_TERMERR is set automatically in Make v4.1+, but macOS is still shipping
# v3.81.
MAKE_TERMERR ?= $(shell [[ -t 2 ]] && echo true)

# This is how you get a literal space into a Makefile.
space := $(eval) $(eval)

# Color support.
yellow = $(shell { tput setaf 3 || tput AF 3; } 2>/dev/null)
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,yellow)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)

# Force vendor directory to rebuild.
.PHONY: vendor_rebuild
vendor_rebuild: bin/.submodules-initialized
	# Use -mod=mod, as -mod=vendor will try install from the vendor directory
	# which may be mismatching upon rebuild.
	$(GO_INSTALL) -v -mod=mod github.com/goware/modvendor
	./build/vendor_rebuild.sh

# Tell Make to delete the target if its recipe fails. Otherwise, if a recipe
# modifies its target before failing, the target's timestamp will make it appear
# up-to-date on the next invocation of Make, even though it is likely corrupt.
# See: https://www.gnu.org/software/make/manual/html_node/Errors.html#Errors
.DELETE_ON_ERROR:

# Targets that name a real file that must be rebuilt on every Make invocation
# should depend on .ALWAYS_REBUILD. (.PHONY should only be used on targets that
# don't name a real file because .DELETE_ON_ERROR does not apply to .PHONY
# targets.)
.ALWAYS_REBUILD:
.PHONY: .ALWAYS_REBUILD

ifneq ($(GIT_DIR),)
# If we're in a git worktree, the git hooks directory may not be in our root,
# so we ask git for the location.
#
# Note that `git rev-parse --git-path hooks` requires git 2.5+.
GITHOOKS := $(subst githooks/,$(GITHOOKSDIR)/,$(wildcard githooks/*))
$(GITHOOKSDIR)/%: githooks/%
	@echo installing $<
	@rm -f $@
	@mkdir -p $(dir $@)
	@ln -s ../../$(basename $<) $(dir $@)
endif

.SECONDARY: pkg/ui/yarn.installed
pkg/ui/yarn.installed: pkg/ui/package.json pkg/ui/yarn.lock pkg/ui/yarn.protobufjs-cli.lock | bin/.submodules-initialized
	$(NODE_RUN) -C pkg/ui yarn install --offline
	# Prevent ProtobufJS from trying to install its own packages because a) the
	# the feature is buggy, and b) it introduces an unnecessary dependency on NPM.
	# See: https://github.com/dcodeIO/protobuf.js/issues/716.
	# We additionally pin the dependencies by linking in a lock file for
	# reproducable builds.
	$(NODE_RUN) pkg/ui/bin/gen-protobuf-cli-deps.js > pkg/ui/node_modules/protobufjs/cli/package.json
	ln -sf ../../../yarn.protobufjs-cli.lock pkg/ui/node_modules/protobufjs/cli/yarn.lock
	$(NODE_RUN) -C pkg/ui/node_modules/protobufjs/cli yarn install --offline
	@# We remove this broken dependency again in pkg/ui/webpack.config.js.
	@# See the comment there for details.
	rm -rf pkg/ui/node_modules/@types/node
	touch $@

# Update the git hooks and install commands from dependencies whenever they
# change.
# These should be synced with `./pkg/cmd/import-tools/main.go`.
bin/.bootstrap: $(GITHOOKS) | bin/.submodules-initialized
	@$(GO_INSTALL) -v \
		github.com/client9/misspell/cmd/misspell \
		github.com/cockroachdb/crlfmt \
		github.com/cockroachdb/gostdlib/cmd/gofmt \
		github.com/cockroachdb/gostdlib/x/tools/cmd/goimports \
		github.com/cockroachdb/stress \
		github.com/goware/modvendor \
		github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway \
		github.com/kevinburke/go-bindata/go-bindata \
		github.com/kisielk/errcheck \
		github.com/mattn/goveralls \
		github.com/mibk/dupl \
		github.com/mmatczuk/go_generics/cmd/go_generics \
		github.com/wadey/gocovmerge \
		golang.org/x/lint/golint \
		golang.org/x/perf/cmd/benchstat \
		golang.org/x/tools/cmd/goyacc \
		golang.org/x/tools/cmd/stringer \
		golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow \
		honnef.co/go/tools/cmd/staticcheck
	touch $@

IGNORE_GOVERS :=

# The following section handles building our C/C++ dependencies. These are
# common because both the root Makefile and protobuf.mk have C dependencies.

host-is-macos := $(findstring Darwin,$(UNAME))
host-is-mingw := $(findstring MINGW,$(UNAME))

ifdef host-is-macos
# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
export MACOSX_DEPLOYMENT_TARGET ?= $(macos-version)
endif

# Cross-compilation occurs when you set TARGET_TRIPLE to something other than
# HOST_TRIPLE. You'll need to ensure the cross-compiling toolchain is on your
# path and override the rest of the variables that immediately follow as
# necessary. For an example, see build/builder/cmd/mkrelease, which sets these
# variables appropriately for the toolchains baked into the builder image.
TARGET_TRIPLE := $(HOST_TRIPLE)
XCMAKE_SYSTEM_NAME :=
XGOOS :=
XGOARCH :=
XCC := $(TARGET_TRIPLE)-cc
XCXX := $(TARGET_TRIPLE)-c++
EXTRA_XCMAKE_FLAGS :=
EXTRA_XCONFIGURE_FLAGS :=

ifneq ($(HOST_TRIPLE),$(TARGET_TRIPLE))
is-cross-compile := 1
endif

target-is-windows := $(findstring w64,$(TARGET_TRIPLE))
target-is-macos := $(findstring darwin,$(TARGET_TRIPLE))

# CMAKE_TARGET_MESSAGES=OFF prevents CMake from printing progress messages
# whenever a target is fully built to prevent spammy output from make when
# c-deps are all already built. Progress messages are still printed when actual
# compilation is being performed.
cmake-flags := -DCMAKE_TARGET_MESSAGES=OFF $(if $(host-is-mingw),-G 'MSYS Makefiles')
configure-flags :=

# Use xcmake-flags when invoking CMake on libraries/binaries for the target
# platform (i.e., the cross-compiled platform, if specified); use plain
# cmake-flags when invoking CMake on libraries/binaries for the host platform.
# Similarly for xconfigure-flags and configure-flags, and xgo and GO.
xcmake-flags := $(cmake-flags) $(EXTRA_XCMAKE_FLAGS)
xconfigure-flags := $(configure-flags) $(EXTRA_XCONFIGURE_FLAGS)

# If we're cross-compiling, inform Autotools and CMake.
ifdef is-cross-compile
xconfigure-flags += --host=$(TARGET_TRIPLE) CC=$(XCC) CXX=$(XCXX)
xcmake-flags += -DCMAKE_SYSTEM_NAME=$(XCMAKE_SYSTEM_NAME) -DCMAKE_C_COMPILER=$(XCC) -DCMAKE_CXX_COMPILER=$(XCXX)
override xgo := GOFLAGS= GOOS=$(XGOOS) GOARCH=$(XGOARCH) CC=$(XCC) CXX=$(XCXX) $(xgo)
endif

C_DEPS_DIR := $(abspath c-deps)
CRYPTOPP_SRC_DIR := $(C_DEPS_DIR)/cryptopp
JEMALLOC_SRC_DIR := $(C_DEPS_DIR)/jemalloc
PROTOBUF_SRC_DIR := $(C_DEPS_DIR)/protobuf
ROCKSDB_SRC_DIR  := $(C_DEPS_DIR)/rocksdb
SNAPPY_SRC_DIR   := $(C_DEPS_DIR)/snappy
GEOS_SRC_DIR     := $(C_DEPS_DIR)/geos
PROJ_SRC_DIR     := $(C_DEPS_DIR)/proj
LIBEDIT_SRC_DIR  := $(C_DEPS_DIR)/libedit
LIBROACH_SRC_DIR := $(C_DEPS_DIR)/libroach
KRB5_SRC_DIR     := $(C_DEPS_DIR)/krb5

# Derived build variants.
use-stdmalloc          := $(findstring stdmalloc,$(TAGS))
use-msan               := $(findstring msan,$(GOFLAGS))

# User-requested build variants.
ENABLE_LIBROACH_ASSERTIONS ?=
ENABLE_ROCKSDB_ASSERTIONS ?=

BUILD_DIR := $(GOPATH)/native/$(TARGET_TRIPLE)

# In MinGW, cgo flags don't handle Unix-style paths, so convert our base path to
# a Windows-style path.
#
# TODO(benesch): Figure out why. MinGW transparently converts Unix-style paths
# everywhere else.
ifdef host-is-mingw
BUILD_DIR := $(shell cygpath -m $(BUILD_DIR))
endif

CRYPTOPP_DIR := $(BUILD_DIR)/cryptopp$(if $(use-msan),_msan)
JEMALLOC_DIR := $(BUILD_DIR)/jemalloc$(if $(use-msan),_msan)
PROTOBUF_DIR := $(BUILD_DIR)/protobuf$(if $(use-msan),_msan)
ROCKSDB_DIR  := $(BUILD_DIR)/rocksdb$(if $(use-msan),_msan)$(if $(use-stdmalloc),_stdmalloc)$(if $(ENABLE_ROCKSDB_ASSERTIONS),_assert)
SNAPPY_DIR   := $(BUILD_DIR)/snappy$(if $(use-msan),_msan)
GEOS_DIR     := $(BUILD_DIR)/geos$(if $(use-msan),_msan)
PROJ_DIR     := $(BUILD_DIR)/proj$(if $(use-msan),_msan)
LIBEDIT_DIR  := $(BUILD_DIR)/libedit$(if $(use-msan),_msan)
LIBROACH_DIR := $(BUILD_DIR)/libroach$(if $(use-msan),_msan)$(if $(ENABLE_LIBROACH_ASSERTIONS),_assert)
KRB5_DIR     := $(BUILD_DIR)/krb5$(if $(use-msan),_msan)
# Can't share with protobuf because protoc is always built for the host.
PROTOC_DIR := $(GOPATH)/native/$(HOST_TRIPLE)/protobuf

LIBCRYPTOPP := $(CRYPTOPP_DIR)/libcryptopp.a
LIBJEMALLOC := $(JEMALLOC_DIR)/lib/libjemalloc.a
LIBPROTOBUF := $(PROTOBUF_DIR)/libprotobuf.a
LIBROCKSDB  := $(ROCKSDB_DIR)/librocksdb.a
LIBSNAPPY   := $(SNAPPY_DIR)/libsnappy.a
LIBEDIT     := $(LIBEDIT_DIR)/src/.libs/libedit.a
LIBROACH    := $(LIBROACH_DIR)/libroach.a
LIBROACHCCL := $(LIBROACH_DIR)/libroachccl.a
LIBPROJ     := $(PROJ_DIR)/lib/libproj$(if $(target-is-windows),_4_9).a
LIBKRB5     := $(KRB5_DIR)/lib/libgssapi_krb5.a
PROTOC      := $(PROTOC_DIR)/protoc

DYN_LIB_DIR := lib
DYN_EXT     := so
ifdef target-is-macos
DYN_EXT     := dylib
endif
ifdef target-is-windows
DYN_EXT     := dll
endif

LIBGEOS     := $(DYN_LIB_DIR)/libgeos.$(DYN_EXT)

C_LIBS_COMMON = \
	$(if $(use-stdmalloc),,$(LIBJEMALLOC)) \
	$(if $(target-is-windows),,$(LIBEDIT)) \
	$(LIBPROTOBUF) $(LIBSNAPPY) $(LIBROCKSDB) $(LIBPROJ) $(LIBGEOS)
C_LIBS_OSS = $(C_LIBS_COMMON) $(LIBROACH)
C_LIBS_CCL = $(C_LIBS_COMMON) $(LIBCRYPTOPP) $(LIBROACHCCL)

# We only include krb5 on linux, non-musl builds.
ifeq "$(findstring linux-gnu,$(TARGET_TRIPLE))" "linux-gnu"
C_LIBS_CCL += $(LIBKRB5)
KRB_CPPFLAGS := $(KRB5_DIR)/include
KRB_DIR := $(KRB5_DIR)/lib
override TAGS += gss
endif

# Go does not permit dashes in build tags. This is undocumented.
native-tag := $(subst -,_,$(TARGET_TRIPLE))$(if $(use-stdmalloc),_stdmalloc)$(if $(use-msan),_msan)

# In each package that uses cgo, we inject include and library search paths into
# files named zcgo_flags_{native-tag}.go. The logic for this is complicated so
# that Make-driven builds can cache the state of builds for multiple
# configurations at once, while still allowing the use of `go build` and `go
# test` for the configuration most recently built with Make.
#
# Building with Make always adds the `make` and {native-tag} tags to the build.
#
# Unsuffixed flags files (zcgo_flags.cgo) have the build constraint `!make` and
# are only compiled when invoking the Go toolchain directly on a package-- i.e.,
# when the `make` build tag is not specified. These files are rebuilt whenever
# the build signature changes (see build/defs.mk.sig), and so reflect the target
# triple that Make was most recently invoked with.
#
# Suffixed flags files (e.g. zcgo_flags_{native-tag}.go) have the build
# constraint `{native-tag}` and are built the first time a Make-driven build
# encounters a given native tag or when the build signature changes (see
# build/defs.mk.sig). These tags are unset when building with the Go toolchain
# directly, so these files are only compiled when building with Make.
CGO_PKGS := \
	pkg/cli \
	pkg/server/status \
	pkg/storage \
	pkg/ccl/storageccl/engineccl \
	pkg/ccl/gssapiccl \
	pkg/geo/geoproj \
	vendor/github.com/knz/go-libedit/unix
vendor/github.com/knz/go-libedit/unix-package := libedit_unix
CGO_UNSUFFIXED_FLAGS_FILES := $(addprefix ./,$(addsuffix /zcgo_flags.go,$(CGO_PKGS)))
CGO_SUFFIXED_FLAGS_FILES   := $(addprefix ./,$(addsuffix /zcgo_flags_$(native-tag).go,$(CGO_PKGS)))
BASE_CGO_FLAGS_FILES := $(CGO_UNSUFFIXED_FLAGS_FILES) $(CGO_SUFFIXED_FLAGS_FILES)
CGO_FLAGS_FILES := $(BASE_CGO_FLAGS_FILES) vendor/github.com/knz/go-libedit/unix/zcgo_flags_extra.go

$(BASE_CGO_FLAGS_FILES): Makefile build/defs.mk.sig | bin/.submodules-initialized
	@echo "regenerating $@"
	@echo '// GENERATED FILE DO NOT EDIT' > $@
	@echo >> $@
	@echo '// +build $(if $(findstring $(native-tag),$@),$(native-tag),!make)' >> $@
	@echo >> $@
	@echo 'package $(if $($(@D)-package),$($(@D)-package),$(notdir $(@D)))' >> $@
	@echo >> $@
	@echo '// #cgo CPPFLAGS: $(addprefix -I,$(JEMALLOC_DIR)/include $(KRB_CPPFLAGS))' >> $@
	@echo '// #cgo LDFLAGS: $(addprefix -L,$(CRYPTOPP_DIR) $(PROTOBUF_DIR) $(JEMALLOC_DIR)/lib $(SNAPPY_DIR) $(LIBEDIT_DIR)/src/.libs $(ROCKSDB_DIR) $(LIBROACH_DIR) $(KRB_DIR) $(PROJ_DIR)/lib)' >> $@
	@echo 'import "C"' >> $@

vendor/github.com/knz/go-libedit/unix/zcgo_flags_extra.go: Makefile | bin/.submodules-initialized
	@echo "regenerating $@"
	@echo '// GENERATED FILE DO NOT EDIT' > $@
	@echo >> $@
	@echo 'package $($(@D)-package)' >> $@
	@echo >> $@
	@echo '// #cgo CPPFLAGS: -DGO_LIBEDIT_NO_BUILD' >> $@
	@echo '// #cgo !windows LDFLAGS: -ledit -lncurses' >> $@
	@echo 'import "C"' >> $@

# BUILD ARTIFACT CACHING
#
# We need to ensure that changes to a dependency's configure or CMake flags
# below cause the corresponding dependency to be rebuilt. It would be correct to
# have the dependencies list this file itself as a prerequisite, but *all*
# dependencies would be rebuilt, likely unnecessarily, whenever this file
# changed. Instead, we give each dependency its own marker file, DEP-rebuild, as
# a prerequisite.
#
# It's not important *what* goes in the marker file, so long as its contents
# change in the same commit as the configure flags. This causes Git to touch the
# marker file when switching between revisions that span the change. For
# simplicity, just sequentially bump the version number within.
#
# NB: the recipes below nuke *all* build artifacts when a dependency's configure
# flags change. In theory, we could rely on the dependency's build system to
# only rebuild the affected objects, but in practice dependencies on configure
# flags are not tracked correctly, and these stale artifacts can cause
# particularly hard-to-debug errors.
#
# Flags needed to make cryptopp to runtime detection of AES cpu instruction sets.
# pclmul and ssse3 need to be defined for the overall AES switch but are only used
# in GCM mode (not currently in use by cockroach).
$(CRYPTOPP_DIR)/Makefile: aes := $(if $(findstring x86_64,$(TARGET_TRIPLE)),-maes -mpclmul -mssse3)
$(CRYPTOPP_DIR)/Makefile: $(C_DEPS_DIR)/cryptopp-rebuild | bin/.submodules-initialized
	rm -rf $(CRYPTOPP_DIR)
	mkdir -p $(CRYPTOPP_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/cryptopp-rebuild. See above for rationale.
	cd $(CRYPTOPP_DIR) && CFLAGS+=" $(aes)" && CXXFLAGS+=" $(aes)" cmake $(xcmake-flags) $(CRYPTOPP_SRC_DIR) \
	  -DCMAKE_BUILD_TYPE=Release

$(JEMALLOC_SRC_DIR)/configure.ac: | bin/.submodules-initialized

$(JEMALLOC_SRC_DIR)/configure: $(JEMALLOC_SRC_DIR)/configure.ac
	cd $(JEMALLOC_SRC_DIR) && autoconf

$(JEMALLOC_DIR)/Makefile: $(C_DEPS_DIR)/jemalloc-rebuild $(JEMALLOC_SRC_DIR)/configure
	rm -rf $(JEMALLOC_DIR)
	mkdir -p $(JEMALLOC_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/jemalloc-rebuild. See above for rationale.
	cd $(JEMALLOC_DIR) && $(JEMALLOC_SRC_DIR)/configure $(xconfigure-flags) --enable-prof

$(KRB5_SRC_DIR)/src/configure.in: | bin/.submodules-initialized

$(KRB5_SRC_DIR)/src/configure: $(KRB5_SRC_DIR)/src/configure.in
	cd $(KRB5_SRC_DIR)/src && autoreconf

$(KRB5_DIR)/Makefile: $(C_DEPS_DIR)/krb5-rebuild $(KRB5_SRC_DIR)/src/configure
	rm -rf $(KRB5_DIR)
	mkdir -p $(KRB5_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/krb5-rebuild. See above for rationale.
	@# If CFLAGS is set to -g1 then make will fail. Use "env -" to clear the environment.
	cd $(KRB5_DIR) && env -u CFLAGS -u CXXFLAGS $(KRB5_SRC_DIR)/src/configure $(xconfigure-flags) --enable-static --disable-shared

$(PROTOBUF_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild | bin/.submodules-initialized
	rm -rf $(PROTOBUF_DIR)
	mkdir -p $(PROTOBUF_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOBUF_DIR) && cmake $(xcmake-flags) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake \
	  -DCMAKE_BUILD_TYPE=Release

ifneq ($(PROTOC_DIR),$(PROTOBUF_DIR))
$(PROTOC_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild | bin/.submodules-initialized
	rm -rf $(PROTOC_DIR)
	mkdir -p $(PROTOC_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOC_DIR) && cmake $(CMAKE_FLAGS) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake \
	  -DCMAKE_BUILD_TYPE=Release
endif

$(ROCKSDB_DIR)/Makefile: sse := $(if $(findstring x86_64,$(TARGET_TRIPLE)),-msse3)
$(ROCKSDB_DIR)/Makefile: $(C_DEPS_DIR)/rocksdb-rebuild | bin/.submodules-initialized $(LIBSNAPPY) $(if $(use-stdmalloc),,$(LIBJEMALLOC))
	rm -rf $(ROCKSDB_DIR)
	mkdir -p $(ROCKSDB_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/rocksdb-rebuild. See above for rationale.
	cd $(ROCKSDB_DIR) && CFLAGS+=" $(sse)" && CXXFLAGS+=" $(sse)" && cmake $(xcmake-flags) $(ROCKSDB_SRC_DIR) \
	  $(if $(findstring release,$(BUILDTYPE)),-DPORTABLE=ON) -DWITH_GFLAGS=OFF \
	  -DSNAPPY_LIBRARIES=$(LIBSNAPPY) -DSNAPPY_INCLUDE_DIR="$(SNAPPY_SRC_DIR);$(SNAPPY_DIR)" -DWITH_SNAPPY=ON \
	  $(if $(use-stdmalloc),,-DJEMALLOC_LIBRARIES=$(LIBJEMALLOC) -DJEMALLOC_INCLUDE_DIR=$(JEMALLOC_DIR)/include -DWITH_JEMALLOC=ON) \
	  -DCMAKE_BUILD_TYPE=$(if $(ENABLE_ROCKSDB_ASSERTIONS),Debug,Release) \
	  -DUSE_RTTI=1 -DFAIL_ON_WARNINGS=0

$(SNAPPY_DIR)/Makefile: $(C_DEPS_DIR)/snappy-rebuild | bin/.submodules-initialized
	rm -rf $(SNAPPY_DIR)
	mkdir -p $(SNAPPY_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/snappy-rebuild. See above for rationale.
	cd $(SNAPPY_DIR) && cmake $(xcmake-flags) $(SNAPPY_SRC_DIR) \
	  -DCMAKE_BUILD_TYPE=Release

$(GEOS_DIR)/Makefile: $(C_DEPS_DIR)/geos-rebuild | bin/.submodules-initialized
	rm -rf $(GEOS_DIR)
	mkdir -p $(GEOS_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/geos-rebuild. See above for rationale.
	cd $(GEOS_DIR) && \
	  cmake $(xcmake-flags) $(GEOS_SRC_DIR) -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_FLAGS=-fPIC -DCMAKE_CXX_FLAGS=-fPIC
	@# Copy geos/export.h to the capi include directory to avoid needing multiple include
	@# directories.
	mkdir $(GEOS_DIR)/capi/geos
	cp $(GEOS_SRC_DIR)/include/geos/export.h $(GEOS_DIR)/capi/geos

$(PROJ_DIR)/Makefile: $(C_DEPS_DIR)/proj-rebuild | bin/.submodules-initialized
	rm -rf $(PROJ_DIR)
	mkdir -p $(PROJ_DIR)
	cd $(PROJ_DIR) && cmake  $(xcmake-flags) $(PROJ_SRC_DIR) -DCMAKE_BUILD_TYPE=Release -DBUILD_LIBPROJ_SHARED=OFF

$(LIBEDIT_SRC_DIR)/configure.ac: | bin/.submodules-initialized

$(LIBEDIT_SRC_DIR)/configure: $(LIBEDIT_SRC_DIR)/configure.ac
	cd $(LIBEDIT_SRC_DIR) && autoconf

$(LIBEDIT_DIR)/Makefile: $(C_DEPS_DIR)/libedit-rebuild $(LIBEDIT_SRC_DIR)/configure
	rm -rf $(LIBEDIT_DIR)
	mkdir -p $(LIBEDIT_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/libedit-rebuild. See above for rationale.
	cd $(LIBEDIT_DIR) && $(LIBEDIT_SRC_DIR)/configure $(xconfigure-flags) --disable-examples --disable-shared

# TODO(benesch): make it possible to build libroach without CCL code. Because
# libroach and libroachccl are defined in the same CMake project, CMake requires
# that the CCL code be present even if only the OSS target will be built.
$(LIBROACH_DIR)/Makefile: $(C_DEPS_DIR)/libroach-rebuild | bin/.submodules-initialized bin/.cpp_protobuf_sources bin/.cpp_ccl_protobuf_sources
	rm -rf $(LIBROACH_DIR)
	mkdir -p $(LIBROACH_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/libroach-rebuild. See above for rationale.
	cd $(LIBROACH_DIR) && cmake $(xcmake-flags) $(LIBROACH_SRC_DIR) \
		-DCMAKE_BUILD_TYPE=$(if $(ENABLE_LIBROACH_ASSERTIONS),Debug,Release) \
		-DPROTOBUF_LIB=$(LIBPROTOBUF) -DROCKSDB_LIB=$(LIBROCKSDB) \
		-DJEMALLOC_LIB=$(LIBJEMALLOC) -DSNAPPY_LIB=$(LIBSNAPPY) \
		-DCRYPTOPP_LIB=$(LIBCRYPTOPP)

# Most of our C and C++ dependencies use Makefiles that are generated by CMake,
# which are rather slow, taking upwards of 500ms to determine that nothing has
# changed. The no-op case is the common case, as C and C++ code is modified
# rarely relative to Go code.
#
# So, for speed, we want to avoid invoking our C and C++ dependencies' build
# systems when nothing has changed. We apply a very coarse heuristic that works
# well in practice: if every file in a given library's source tree is older than
# the compiled library, then the compiled library must be up-to-date.
#
# Normally, you'd accomplish this in Make itself by declaring a prerequisite for
# every file in the library's source tree. For example, you'd have a rule like
# this for protoc:
#
#     $(PROTOC): $(PROTOC_DIR)/Makefile $(shell find c-deps/protobuf)
#       $(MAKE) -C $(PROTOC_DIR) protoc
#
# Note the prerequisite that shells out to the 'find' command. Unfortunately,
# this winds up being as slow as unconditionally invoking the child build
# system! The cost of repeated find invocations, one per command, adds up, plus
# Make needs to stat all of the resulting files, and it seems to do so
# sequentially.
#
# Instead, we unconditionally run the recipe for each C and C++ dependency, via
# .ALWAYS_REBUILD, but condition the execution of the dependency's build system
# on the output of uptodate, a Go binary of our own design. uptodate walks and
# stats the directory tree in parallel, and can make the up-to-date
# determination in under 20ms.

$(PROTOC): $(PROTOC_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD | $(LIBPROTOBUF)
	@uptodate $@ $(PROTOBUF_SRC_DIR) || $(MAKE) --no-print-directory -C $(PROTOC_DIR) protoc

$(LIBCRYPTOPP): $(CRYPTOPP_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(CRYPTOPP_SRC_DIR) || $(MAKE) --no-print-directory -C $(CRYPTOPP_DIR) static

$(LIBJEMALLOC): $(JEMALLOC_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(JEMALLOC_SRC_DIR) || $(MAKE) --no-print-directory -C $(JEMALLOC_DIR) build_lib_static

$(LIBPROTOBUF): $(PROTOBUF_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(PROTOBUF_SRC_DIR) || $(MAKE) --no-print-directory -C $(PROTOBUF_DIR) libprotobuf

$(LIBSNAPPY): $(SNAPPY_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(SNAPPY_SRC_DIR) || $(MAKE) --no-print-directory -C $(SNAPPY_DIR) snappy

$(LIBGEOS): $(GEOS_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(GEOS_SRC_DIR) || $(MAKE) --no-print-directory -C $(GEOS_DIR) geos_c
	mkdir -p $(DYN_LIB_DIR)
	rm -f $(DYN_LIB_DIR)/lib{geos,geos_c}.$(DYN_EXT)
	cp -L $(GEOS_DIR)/$(if $(target-is-windows),bin,lib)/lib{geos,geos_c}.$(DYN_EXT) $(DYN_LIB_DIR)

$(LIBPROJ): $(PROJ_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(PROJ_SRC_DIR) || $(MAKE) --no-print-directory -C $(PROJ_DIR) proj

$(LIBEDIT): $(LIBEDIT_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(LIBEDIT_SRC_DIR) || $(MAKE) --no-print-directory -C $(LIBEDIT_DIR)/src

$(LIBROCKSDB): $(ROCKSDB_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(ROCKSDB_SRC_DIR) || $(MAKE) --no-print-directory -C $(ROCKSDB_DIR) rocksdb

libroach-inputs := $(LIBROACH_SRC_DIR) $(ROCKSDB_SRC_DIR)/include $(PROTOBUF_SRC_DIR)/src

$(LIBROACH): $(LIBROACH_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(libroach-inputs) || $(MAKE) --no-print-directory -C $(LIBROACH_DIR) roach

$(LIBROACHCCL): $(LIBROACH_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(libroach-inputs) || $(MAKE) --no-print-directory -C $(LIBROACH_DIR) roachccl

$(LIBKRB5): $(KRB5_DIR)/Makefile bin/uptodate .ALWAYS_REBUILD
	@uptodate $@ $(KRB5_SRC_DIR)/src || $(MAKE) --no-print-directory -C $(KRB5_DIR)

# Convenient names for maintainers. Not used by other targets in the Makefile.
.PHONY: protoc libcryptopp libjemalloc libprotobuf libsnappy libgeos libproj librocksdb libroach libroachccl libkrb5
protoc:      $(PROTOC)
libcryptopp: $(LIBCRYPTOPP)
libedit:     $(LIBEDIT)
libjemalloc: $(LIBJEMALLOC)
libprotobuf: $(LIBPROTOBUF)
libsnappy:   $(LIBSNAPPY)
libgeos:     $(LIBGEOS)
libproj:     $(LIBPROJ)
librocksdb:  $(LIBROCKSDB)
libroach:    $(LIBROACH)
libroachccl: $(LIBROACHCCL)
libkrb5:     $(LIBKRB5)

PHONY: check-libroach
check-libroach: ## Run libroach tests.
check-libroach: $(LIBROACH_DIR)/Makefile $(LIBJEMALLOC) $(LIBPROTOBUF) $(LIBSNAPPY) $(LIBROCKSDB) $(LIBCRYPTOPP)
	@$(MAKE) --no-print-directory -C $(LIBROACH_DIR)
	cd $(LIBROACH_DIR) && ctest -V -R $(TESTS)

override TAGS += make $(native-tag)

# Some targets (protobuf) produce different results depending on the sort order;
# set LC_ALL so this is consistent across systems.
export LC_ALL=C

# defs.mk.sig attempts to capture common cases where defs.mk needs to be
# recomputed, like when compiling for a different platform or using a different
# Go binary. It is not intended to be perfect. Upgrading the compiler toolchain
# in place will go unnoticed, for example. Similar problems exist in all Make-
# based build systems and are not worth solving.
build/defs.mk.sig: sig = $(PATH):$(CURDIR):$(GO):$(GOPATH):$(CC):$(CXX):$(TARGET_TRIPLE):$(BUILDTYPE):$(IGNORE_GOVERS):$(ENABLE_LIBROACH_ASSERTIONS):$(ENABLE_ROCKSDB_ASSERTIONS)
build/defs.mk.sig: .ALWAYS_REBUILD
	@echo '$(sig)' | cmp -s - $@ || echo '$(sig)' > $@

COCKROACH      := ./cockroach$(SUFFIX)
COCKROACHOSS   := ./cockroachoss$(SUFFIX)
COCKROACHSHORT := ./cockroachshort$(SUFFIX)

SQLPARSER_TARGETS = \
	pkg/sql/parser/sql.go \
	pkg/sql/parser/helpmap_test.go \
	pkg/sql/parser/help_messages.go \
	pkg/sql/lex/tokens.go \
	pkg/sql/lex/keywords.go \
	pkg/sql/lex/reserved_keywords.go

PROTOBUF_TARGETS := bin/.go_protobuf_sources bin/.gw_protobuf_sources bin/.cpp_protobuf_sources bin/.cpp_ccl_protobuf_sources

DOCGEN_TARGETS := bin/.docgen_bnfs bin/.docgen_functions docs/generated/redact_safe.md

EXECGEN_TARGETS = \
  pkg/col/coldata/vec.eg.go \
  pkg/sql/colexec/and_or_projection.eg.go \
  pkg/sql/colexec/cast.eg.go \
  pkg/sql/colexec/const.eg.go \
  pkg/sql/colexec/distinct.eg.go \
  pkg/sql/colexec/hashjoiner.eg.go \
  pkg/sql/colexec/hashtable_distinct.eg.go \
  pkg/sql/colexec/hashtable_full_default.eg.go \
  pkg/sql/colexec/hashtable_full_deleting.eg.go \
  pkg/sql/colexec/hash_aggregator.eg.go \
  pkg/sql/colexec/hash_any_not_null_agg.eg.go \
  pkg/sql/colexec/hash_avg_agg.eg.go \
  pkg/sql/colexec/hash_bool_and_or_agg.eg.go \
  pkg/sql/colexec/hash_count_agg.eg.go \
  pkg/sql/colexec/hash_min_max_agg.eg.go \
  pkg/sql/colexec/hash_sum_agg.eg.go \
  pkg/sql/colexec/hash_sum_int_agg.eg.go \
  pkg/sql/colexec/hash_utils.eg.go \
  pkg/sql/colexec/like_ops.eg.go \
  pkg/sql/colexec/mergejoinbase.eg.go \
  pkg/sql/colexec/mergejoiner_exceptall.eg.go \
  pkg/sql/colexec/mergejoiner_fullouter.eg.go \
  pkg/sql/colexec/mergejoiner_inner.eg.go \
  pkg/sql/colexec/mergejoiner_intersectall.eg.go \
  pkg/sql/colexec/mergejoiner_leftanti.eg.go \
  pkg/sql/colexec/mergejoiner_leftouter.eg.go \
  pkg/sql/colexec/mergejoiner_leftsemi.eg.go \
  pkg/sql/colexec/mergejoiner_rightouter.eg.go \
  pkg/sql/colexec/ordered_any_not_null_agg.eg.go \
  pkg/sql/colexec/ordered_avg_agg.eg.go \
  pkg/sql/colexec/ordered_bool_and_or_agg.eg.go \
  pkg/sql/colexec/ordered_count_agg.eg.go \
  pkg/sql/colexec/ordered_min_max_agg.eg.go \
  pkg/sql/colexec/ordered_sum_agg.eg.go \
  pkg/sql/colexec/ordered_sum_int_agg.eg.go \
  pkg/sql/colexec/ordered_synchronizer.eg.go \
  pkg/sql/colexec/overloads_test_utils.eg.go \
  pkg/sql/colexec/proj_const_left_ops.eg.go \
  pkg/sql/colexec/proj_const_right_ops.eg.go \
  pkg/sql/colexec/proj_non_const_ops.eg.go \
  pkg/sql/colexec/quicksort.eg.go \
  pkg/sql/colexec/rank.eg.go \
  pkg/sql/colexec/relative_rank.eg.go \
  pkg/sql/colexec/row_number.eg.go \
  pkg/sql/colexec/rowstovec.eg.go \
  pkg/sql/colexec/selection_ops.eg.go \
  pkg/sql/colexec/select_in.eg.go \
  pkg/sql/colexec/sort.eg.go \
  pkg/sql/colexec/substring.eg.go \
  pkg/sql/colexec/values_differ.eg.go \
  pkg/sql/colexec/vec_comparators.eg.go \
  pkg/sql/colexec/vec_to_datum.eg.go \
  pkg/sql/colexec/window_peer_grouper.eg.go

OPTGEN_TARGETS = \
	pkg/sql/opt/memo/expr.og.go \
	pkg/sql/opt/operator.og.go \
	pkg/sql/opt/xform/explorer.og.go \
	pkg/sql/opt/norm/factory.og.go \
	pkg/sql/opt/rule_name.og.go \
	pkg/sql/opt/rule_name_string.go

go-targets-ccl := \
	$(COCKROACH) $(COCKROACHSHORT) \
	bin/workload \
	go-install \
	bench benchshort \
	check test testshort testslow testrace testraceslow testbuild \
	stress stressrace \
	roachprod-stress roachprod-stressrace \
	generate \
	lint lintshort

go-targets := $(go-targets-ccl) $(COCKROACHOSS)

.DEFAULT_GOAL := all
all: build

.PHONY: c-deps
c-deps: $(C_LIBS_CCL)

build-mode = build -o $@

go-install: build-mode = install

$(COCKROACH) go-install generate: pkg/ui/distccl/bindata.go

$(COCKROACHOSS): BUILDTARGET = ./pkg/cmd/cockroach-oss
$(COCKROACHOSS): $(C_LIBS_OSS) pkg/ui/distoss/bindata.go

$(COCKROACHSHORT): BUILDTARGET = ./pkg/cmd/cockroach-short

$(go-targets-ccl): $(C_LIBS_CCL)

BUILDINFO = .buildinfo/tag .buildinfo/rev
BUILD_TAGGED_RELEASE =

## Override for .buildinfo/tag
BUILDINFO_TAG :=

$(go-targets): bin/.bootstrap $(BUILDINFO) $(CGO_FLAGS_FILES) $(PROTOBUF_TARGETS) $(LIBPROJ)
$(go-targets): $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS)
$(go-targets): override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(if $(BUILDINFO_TAG),$(BUILDINFO_TAG),$(shell cat .buildinfo/tag))" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.cgoTargetTriple=$(TARGET_TRIPLE)" \
	$(if $(BUILDCHANNEL),-X "github.com/cockroachdb/cockroach/pkg/build.channel=$(BUILDCHANNEL)") \
	$(if $(BUILD_TAGGED_RELEASE),-X "github.com/cockroachdb/cockroach/pkg/util/log.crashReportEnv=$(if $(BUILDINFO_TAG),$(BUILDINFO_TAG),$(shell cat .buildinfo/tag))")

# The build.utcTime format must remain in sync with TimeFormat in
# pkg/build/info.go. It is not installed in tests to avoid busting the cache on
# every rebuild.
$(COCKROACH) $(COCKROACHOSS) $(COCKROACHSHORT) go-install: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')"

SETTINGS_DOC_PAGE := docs/generated/settings/settings.html

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: go-install
$(COCKROACH) $(COCKROACHOSS) $(COCKROACHSHORT) go-install:
	 $(xgo) $(build-mode) -v $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

# The build targets, in addition to producing a Cockroach binary, silently
# regenerate SQL diagram BNFs and some other doc pages. Generating these docs
# doesn't really belong in the build target, but when they were only part of the
# generate target it was too easy to forget to regenerate them when necessary
# and burn a CI cycle.
#
# We check these docs into version control in the first place in the hope that
# the diff of the generated docs that shows up in Reviewable, 'git diff', etc.
# makes it obvious when a commit has broken the docs. For example, it's very
# easy for changes to the SQL parser to result in unintelligible railroad
# diagrams. When the generated files are not checked in, the breakage goes
# unnoticed until the docs team comes along, potentially months later. Much
# better to make the developer who introduces the breakage fix the breakage.
.PHONY: build buildoss buildshort
build: ## Build the CockroachDB binary.
buildoss: ## Build the CockroachDB binary without any CCL-licensed code.
buildshort: ## Build the CockroachDB binary without the admin UI.
build: $(COCKROACH)
buildoss: $(COCKROACHOSS)
buildshort: $(COCKROACHSHORT)
build buildoss buildshort: $(if $(is-cross-compile),,$(DOCGEN_TARGETS))
build buildshort: $(if $(is-cross-compile),,$(SETTINGS_DOC_PAGE))

# For historical reasons, symlink cockroach to cockroachshort.
# TODO(benesch): see if it would break anyone's workflow to remove this.
buildshort:
	ln -sf $(COCKROACHSHORT) $(COCKROACH)

.PHONY: install
install: ## Install the CockroachDB binary.
install: $(COCKROACH)
	$(INSTALL) -d -m 755 $(DESTDIR)$(bindir)
	$(INSTALL) -m 755 $(COCKROACH) $(DESTDIR)$(bindir)/cockroach

.PHONY: start
start: $(COCKROACH)
start:
	$(COCKROACH) start $(STARTFLAGS)

# Build, but do not run the tests.
# PKG is expanded and all packages are built and moved to their directory.
.PHONY: testbuild
testbuild:
	$(xgo) list -tags '$(TAGS)' -f \
	'$(xgo) test -v $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -c {{.ImportPath}} -o {{.Dir}}/{{.Name}}.test' $(PKG) | \
	$(SHELL)

testshort: override TESTFLAGS += -short

testrace: ## Run tests with the Go race detector enabled.
testrace stressrace roachprod-stressrace: override GOFLAGS += -race
testrace stressrace roachprod-stressrace: export GORACE := halt_on_error=1
testrace stressrace roachprod-stressrace: TESTTIMEOUT := $(RACETIMEOUT)

# Directory scans in the builder image are excruciatingly slow when running
# Docker for Mac, so we filter out the 20k+ UI dependencies that are
# guaranteed to be irrelevant to save nearly 10s on every Make invocation.
FIND_RELEVANT := find ./pkg -name node_modules -prune -o

bench: ## Run benchmarks.
bench benchshort: TESTS := -
bench benchshort: BENCHES := .
bench benchshort: TESTTIMEOUT := $(BENCHTIMEOUT)

# -benchtime=1ns runs one iteration of each benchmark. The -short flag is set so
# that longer running benchmarks can skip themselves.
benchshort: override TESTFLAGS += -benchtime=1ns -short

.PHONY: check test testshort testrace testlogic testbaselogic testccllogic testoptlogic bench benchshort
test: ## Run tests.
check test testshort testrace bench benchshort:
	$(xgo) test $(GOTESTFLAGS) $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

.PHONY: stress stressrace
stress: ## Run tests under stress.
stressrace: ## Run tests under stress with the race detector enabled.
stress stressrace:
	$(xgo) test $(GOTESTFLAGS) $(GOFLAGS) $(GOMODVENDORFLAGS) -exec 'stress $(STRESSFLAGS)' -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" -timeout 0 $(PKG) $(filter-out -v,$(TESTFLAGS)) -v -args -test.timeout $(TESTTIMEOUT)

.PHONY: roachprod-stress roachprod-stressrace
roachprod-stress roachprod-stressrace: bin/roachprod-stress
	# The bootstrap target creates, among other things, ./bin/stress.
	@if [ -z "$(CLUSTER)" ]; then \
		echo "ERROR: missing or empty CLUSTER; create one via:"; \
		echo "roachprod create \$$USER-stress -n 20 --gce-machine-type=n1-standard-8 --local-ssd=false"; \
		exit 1; \
	fi
	build/builder.sh make bin/.bootstrap
	build/builder.sh mkrelease amd64-linux-gnu test GOFLAGS="$(GOFLAGS)" TESTFLAGS="-v -c -o $(notdir $(patsubst %/,%,$(PKG))).test" PKG=$(PKG)
	bin/roachprod-stress $(CLUSTER) $(patsubst github.com/cockroachdb/cockroach/%,./%,$(PKG)) $(STRESSFLAGS) -- \
	  -test.run "$(TESTS)" $(filter-out -v,$(TESTFLAGS)) -test.v -test.timeout $(TESTTIMEOUT); \

testlogic: testbaselogic testoptlogic testccllogic

testbaselogic: ## Run SQL Logic Tests.
testbaselogic: bin/logictest

testccllogic: ## Run SQL CCL Logic Tests.
testccllogic: bin/logictestccl

testoptlogic: ## Run SQL Logic Tests from opt package.
testoptlogic: bin/logictestopt

logic-test-selector := $(if $(TESTCONFIG),^$(TESTCONFIG)$$)/$(if $(FILES),^$(subst $(space),$$|^,$(FILES))$$)/$(SUBTESTS)
testbaselogic: TESTS := TestLogic/$(logic-test-selector)
testccllogic: TESTS := TestCCLLogic/$(logic-test-selector)
testoptlogic: TESTS := TestExecBuild/$(logic-test-selector)

# Note: we specify -config here in addition to the filter on TESTS
# above. This is because if we only restrict in TESTS, this will
# merely cause Go to skip the sub-tests that match the pattern. It
# does not prevent loading and initializing every default config in
# turn (including setting up the test clusters, etc.). By specifying
# -config, the extra initialization overhead is averted.
testbaselogic testccllogic testoptlogic: TESTFLAGS := -test.v $(if $(FILES),-show-sql) $(if $(TESTCONFIG),-config $(TESTCONFIG))
testbaselogic testccllogic testoptlogic:
	cd $($(<F)-package) && $(<F) -test.run "$(TESTS)" -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

testraceslow: override GOFLAGS += -race
testraceslow: TESTTIMEOUT := $(RACETIMEOUT)

.PHONY: testslow testraceslow
testslow testraceslow: override TESTFLAGS += -v
testslow testraceslow:
	$(xgo) test $(GOTESTFLAGS) $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10

.PHONY: upload-coverage
upload-coverage: bin/.bootstrap
	$(GO) install github.com/wadey/gocovmerge
	$(GO) install github.com/mattn/goveralls
	@build/upload-coverage.sh

.PHONY: acceptance
acceptance: TESTTIMEOUT := $(ACCEPTANCETIMEOUT)
acceptance: export TESTTIMEOUT := $(TESTTIMEOUT)
acceptance: ## Run acceptance tests.
	+@pkg/acceptance/run.sh

.PHONY: compose
compose: export TESTTIMEOUT := $(TESTTIMEOUT)
compose: ## Run compose tests.
	+@pkg/compose/run.sh

.PHONY: dupl
dupl: bin/.bootstrap
	$(FIND_RELEVANT) \
	       -name '*.go'             \
	       -not -name '*.pb.go'     \
	       -not -name '*.pb.gw.go'  \
	       -not -name 'bindata.go' \
	       -not -name '*_string.go' \
	       -not -name 'sql.go'      \
	       -not -name 'irgen.go'    \
	       -not -name '*.ir.go'     \
	| dupl -files $(DUPLFLAGS)

.PHONY: generate
generate: ## Regenerate generated code.
generate: protobuf $(DOCGEN_TARGETS) $(OPTGEN_TARGETS) $(SQLPARSER_TARGETS) $(SETTINGS_DOC_PAGE) bin/langgen bin/terraformgen
	$(GO) generate $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(PKG)
	$(MAKE) execgen

lint lintshort: TESTTIMEOUT := $(LINTTIMEOUT)

.PHONY: lint
lint: override TAGS += lint
lint: ## Run all style checkers and linters.
lint: bin/returncheck bin/roachvet bin/optfmt
	@if [ -t 1 ]; then echo '$(yellow)NOTE: `make lint` is very slow! Perhaps `make lintshort`?$(term-reset)'; fi
	@# Run 'go build -i' to ensure we have compiled object files available for all
	@# packages. In Go 1.10, only 'go vet' recompiles on demand. For details:
	@# https://groups.google.com/forum/#!msg/golang-dev/qfa3mHN4ZPA/X2UzjNV1BAAJ.
	$(xgo) build -i -v $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(PKG)
	$(xgo) test $(GOTESTFLAGS) ./pkg/testutils/lint -v $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -timeout $(TESTTIMEOUT) -run 'Lint/$(TESTS)'

.PHONY: lintshort
lintshort: override TAGS += lint
lintshort: ## Run a fast subset of the style checkers and linters.
lintshort: bin/roachvet bin/optfmt
	$(xgo) test $(GOTESTFLAGS) ./pkg/testutils/lint -v $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -timeout $(TESTTIMEOUT) -run 'TestLint/$(TESTS)'

.PHONY: protobuf
protobuf: $(PROTOBUF_TARGETS)
protobuf: ## Regenerate generated code for protobuf definitions.

# pre-push locally runs most of the checks CI will run. Notably, it doesn't run
# the acceptance tests.
.PHONY: pre-push
pre-push: ## Run generate, lint, and test.
pre-push: generate lint test ui-lint ui-test
	! git status --porcelain | read || (git status; git --no-pager diff -a 1>&2; exit 1)

# archive builds a source tarball out of this repository. Files in the special
# directory build/archive/contents are inserted directly into $(ARCHIVE_BASE).
# All other files in the repository are inserted into the archive with prefix
# $(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach to allow the extracted
# archive to serve directly as a GOPATH root.
.PHONY: archive
archive: ## Build a source tarball from this repository.
archive: $(ARCHIVE)

$(ARCHIVE): $(ARCHIVE).tmp
	gzip -c $< > $@

# ARCHIVE_EXTRAS are hard-to-generate files and their prerequisites that are
# pre-generated and distributed in source archives to minimize the number of
# dependencies required for end-users to build from source.
ARCHIVE_EXTRAS = \
	$(BUILDINFO) \
	$(SQLPARSER_TARGETS) \
	$(OPTGEN_TARGETS) \
	pkg/ui/distccl/bindata.go pkg/ui/distoss/bindata.go

# TODO(benesch): Make this recipe use `git ls-files --recurse-submodules`
# instead of scripts/ls-files.sh once Git v2.11 is widely deployed.
.INTERMEDIATE: $(ARCHIVE).tmp
$(ARCHIVE).tmp: ARCHIVE_BASE = cockroach-$(if $(BUILDINFO_TAG),$(BUILDINFO_TAG),$(shell cat .buildinfo/tag))
$(ARCHIVE).tmp: $(ARCHIVE_EXTRAS)
	echo "$(if $(BUILDINFO_TAG),$(BUILDINFO_TAG),$(shell cat .buildinfo/tag))" > .buildinfo/tag
	scripts/ls-files.sh | $(TAR) -cf $@ -T - $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, $^
	(cd build/archive/contents && $(TAR) -rf ../../../$@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/, *)

.buildinfo:
	@mkdir -p $@

# Do not use plumbing commands, like git diff-index, in this target. Our build
# process modifies files quickly enough that plumbing commands report false
# positives on filesystems with only one second of resolution as a performance
# optimization. Porcelain commands, like git diff, exist to detect and remove
# these false positives.
#
# For details, see the "Possible timestamp problems with diff-files?" thread on
# the Git mailing list (http://marc.info/?l=git&m=131687596307197).
.buildinfo/tag: | .buildinfo
	@{ git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD; } | tr -d \\n > $@

.buildinfo/rev: | .buildinfo
	@git rev-parse HEAD > $@

ifneq ($(GIT_DIR),)
# If we're in a Git checkout, we update the buildinfo information on every build
# to keep it up-to-date.
.buildinfo/tag: .ALWAYS_REBUILD
.buildinfo/rev: .ALWAYS_REBUILD
endif

CPP_PROTO_ROOT := $(LIBROACH_SRC_DIR)/protos
CPP_PROTO_CCL_ROOT := $(LIBROACH_SRC_DIR)/protosccl

GOGO_PROTOBUF_PATH := ./vendor/github.com/gogo/protobuf
PROTOBUF_PATH  := $(GOGO_PROTOBUF_PATH)/protobuf

GOGOPROTO_PROTO := $(GOGO_PROTOBUF_PATH)/gogoproto/gogo.proto

ERRORS_PATH := ./vendor/github.com/cockroachdb/errors
ERRORS_PROTO := $(ERRORS_PATH)/errorspb/errors.proto

COREOS_PATH := ./vendor/go.etcd.io

GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := ./vendor/$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes to the Go package containing the generated Go code.
PROTO_MAPPINGS :=
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,

GW_SERVER_PROTOS := ./pkg/server/serverpb/admin.proto ./pkg/server/serverpb/status.proto ./pkg/server/serverpb/authentication.proto
GW_TS_PROTOS := ./pkg/ts/tspb/timeseries.proto

GW_PROTOS  := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)
GW_SOURCES := $(GW_PROTOS:%.proto=%.pb.gw.go)

GO_PROTOS := $(sort $(shell $(FIND_RELEVANT) -type f -name '*.proto' -print))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

PBJS := $(NODE_RUN) pkg/ui/node_modules/.bin/pbjs
PBTS := $(NODE_RUN) pkg/ui/node_modules/.bin/pbts

# Unlike the protobuf compiler for Go and C++, the protobuf compiler for
# JavaScript only needs the entrypoint protobufs to be listed. It automatically
# compiles any protobufs the entrypoints depend upon.
JS_PROTOS_CCL := $(filter %/ccl/storageccl/engineccl/enginepbccl/stats.proto,$(GO_PROTOS))
UI_JS_CCL := pkg/ui/ccl/src/js/protos.js
UI_TS_CCL := pkg/ui/ccl/src/js/protos.d.ts
UI_PROTOS_CCL := $(UI_JS_CCL) $(UI_TS_CCL)

UI_JS_OSS := pkg/ui/src/js/protos.js
UI_TS_OSS := pkg/ui/src/js/protos.d.ts
UI_PROTOS_OSS := $(UI_JS_OSS) $(UI_TS_OSS)

CPP_PROTOS := $(filter %/roachpb/api.proto %/roachpb/metadata.proto %/roachpb/data.proto %/roachpb/internal.proto %/roachpb/errors.proto %util/tracing/recorded_span.proto %/concurrency/lock/locking.proto %/enginepb/mvcc.proto %/enginepb/mvcc3.proto %/enginepb/file_registry.proto %/enginepb/rocksdb.proto %/hlc/legacy_timestamp.proto %/hlc/timestamp.proto %/log/log.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst ./pkg,$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.h))
CPP_SOURCES := $(subst ./pkg,$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.cc))

CPP_PROTOS_CCL := $(filter %/ccl/baseccl/encryption_options.proto %/ccl/storageccl/engineccl/enginepbccl/key_registry.proto %/ccl/storageccl/engineccl/enginepbccl/stats.proto,$(GO_PROTOS))
CPP_HEADERS_CCL := $(subst ./pkg,$(CPP_PROTO_CCL_ROOT),$(CPP_PROTOS_CCL:%.proto=%.pb.h))
CPP_SOURCES_CCL := $(subst ./pkg,$(CPP_PROTO_CCL_ROOT),$(CPP_PROTOS_CCL:%.proto=%.pb.cc))

$(GOGOPROTO_PROTO): bin/.submodules-initialized
$(ERRORS_PROTO): bin/.submodules-initialized

bin/.go_protobuf_sources: $(PROTOC) $(GO_PROTOS) $(GOGOPROTO_PROTO) $(ERRORS_PROTO) bin/.bootstrap bin/protoc-gen-gogoroach
	$(FIND_RELEVANT) -type f -name '*.pb.go' -exec rm {} +
	set -e; for dir in $(sort $(dir $(GO_PROTOS))); do \
	  build/werror.sh $(PROTOC) -Ipkg:./vendor/github.com:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH):$(ERRORS_PATH) --gogoroach_out=$(PROTO_MAPPINGS),plugins=grpc,import_prefix=github.com/cockroachdb/cockroach/pkg/:./pkg $$dir/*.proto; \
	done
	$(SED_INPLACE) -E \
		-e '/import _ /d' \
		-e 's!import (fmt|math) "github.com/cockroachdb/cockroach/pkg/(fmt|math)"! !g' \
		-e 's!github\.com/cockroachdb/cockroach/pkg/(etcd)!go.etcd.io/\1!g' \
		-e 's!github.com/cockroachdb/cockroach/pkg/((bytes|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)([^a-z]|$$))!\1!g' \
        -e 's!github.com/cockroachdb/cockroach/pkg/errorspb!github.com/cockroachdb/errors/errorspb!g' \
		-e 's!golang.org/x/net/context!context!g' \
		$(GO_SOURCES)
	@# TODO(benesch): Remove the last sed command after https://github.com/grpc/grpc-go/issues/711.
	gofmt -s -w $(GO_SOURCES)
	touch $@

bin/.gw_protobuf_sources: $(PROTOC) $(GW_SERVER_PROTOS) $(GW_TS_PROTOS) $(GO_PROTOS) $(GOGOPROTO_PROTO) $(ERRORS_PROTO) bin/.bootstrap
	$(FIND_RELEVANT) -type f -name '*.pb.gw.go' -exec rm {} +
	build/werror.sh $(PROTOC) -Ipkg:./vendor/github.com:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(ERRORS_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:./pkg $(GW_SERVER_PROTOS)
	build/werror.sh $(PROTOC) -Ipkg:./vendor/github.com:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(ERRORS_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:./pkg $(GW_TS_PROTOS)
	@# TODO(benesch): Remove after https://github.com/grpc/grpc-go/issues/711.
	$(SED_INPLACE) -E 's!golang.org/x/net/context!context!g' $(GW_SOURCES)
	gofmt -s -w $(GW_SOURCES)
	@# TODO(jordan,benesch) This can be removed along with the above TODO.
	goimports -w $(GW_SOURCES)
	touch $@

bin/.cpp_protobuf_sources: $(PROTOC) $(CPP_PROTOS)
	rm -rf $(CPP_PROTO_ROOT)
	mkdir -p $(CPP_PROTO_ROOT)
	build/werror.sh $(PROTOC) -Ipkg:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(CPP_PROTO_ROOT) $(CPP_PROTOS)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS) $(CPP_SOURCES)
	touch $@

bin/.cpp_ccl_protobuf_sources: $(PROTOC) $(CPP_PROTOS_CCL)
	rm -rf $(CPP_PROTO_CCL_ROOT)
	mkdir -p $(CPP_PROTO_CCL_ROOT)
	build/werror.sh $(PROTOC) -Ipkg:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(CPP_PROTO_CCL_ROOT) $(CPP_PROTOS_CCL)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS_CCL) $(CPP_SOURCES_CCL)
	touch $@

# The next two rules must be kept exactly the same except the CCL one depends
# on one additional proto. They generate the pbjs files from the protobuf
# definitions, which then act is inputs to the pbts compiler, which creates
# typescript definitions for the proto files afterwards.

.SECONDARY: $(UI_JS_CCL)
$(UI_JS_CCL): $(GW_PROTOS) $(GO_PROTOS) $(JS_PROTOS_CCL) pkg/ui/yarn.installed | bin/.submodules-initialized
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBJS) -t static-module -w es6 --strict-long --keep-case --path pkg --path ./vendor/github.com --path $(GOGO_PROTOBUF_PATH) --path $(ERRORS_PATH) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(filter %.proto,$(GW_PROTOS) $(JS_PROTOS_CCL)) >> $@

.SECONDARY: $(UI_JS_OSS)
$(UI_JS_OSS): $(GW_PROTOS) $(GO_PROTOS) pkg/ui/yarn.installed | bin/.submodules-initialized
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBJS) -t static-module -w es6 --strict-long --keep-case --path pkg --path ./vendor/github.com --path $(GOGO_PROTOBUF_PATH) --path $(ERRORS_PATH) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(filter %.proto,$(GW_PROTOS)) >> $@

# End of PBJS-generated files.

.SECONDARY: $(UI_TS_OSS) $(UI_TS_CCL)
protos%.d.ts: protos%.js pkg/ui/yarn.installed
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBTS) $< >> $@

STYLINT            := ./node_modules/.bin/stylint
TSLINT             := ./node_modules/.bin/tslint
TSC                := ./node_modules/.bin/tsc
KARMA              := ./node_modules/.bin/karma
WEBPACK            := ./node_modules/.bin/webpack
WEBPACK_DEV_SERVER := ./node_modules/.bin/webpack-dev-server
WEBPACK_DASHBOARD  := ./opt/node_modules/.bin/webpack-dashboard

.PHONY: ui-generate
ui-generate: pkg/ui/distccl/bindata.go

.PHONY: ui-fonts
ui-fonts:
	pkg/ui/scripts/font-gen

.PHONY: ui-topo
ui-topo: pkg/ui/yarn.installed
	pkg/ui/scripts/topo.js

.PHONY: ui-lint
ui-lint: pkg/ui/yarn.installed $(UI_PROTOS_OSS) $(UI_PROTOS_CCL)
	$(NODE_RUN) -C pkg/ui $(STYLINT) -c .stylintrc styl
	$(NODE_RUN) -C pkg/ui $(TSLINT) -c tslint.json -p tsconfig.json
	@# TODO(benesch): Invoke tslint just once when palantir/tslint#2827 is fixed.
	$(NODE_RUN) -C pkg/ui $(TSLINT) -c tslint.json *.js
	$(NODE_RUN) -C pkg/ui $(TSC)
	@if $(NODE_RUN) -C pkg/ui yarn list | grep phantomjs; then echo ^ forbidden UI dependency >&2; exit 1; fi

# DLLs are Webpack bundles, not Windows shared libraries. See "DLLs for speedy
# builds" in the UI README for details.
UI_CCL_DLLS := pkg/ui/dist/protos.ccl.dll.js pkg/ui/dist/vendor.oss.dll.js
UI_CCL_MANIFESTS := pkg/ui/protos.ccl.manifest.json pkg/ui/vendor.oss.manifest.json
UI_OSS_DLLS := $(subst .ccl,.oss,$(UI_CCL_DLLS))
UI_OSS_MANIFESTS := $(subst .ccl,.oss,$(UI_CCL_MANIFESTS))

# (Ab)use pattern rules to teach Make that this one Webpack command produces two
# files. Normally, Make would run the recipe twice if dist/FOO.js and
# FOO-manifest.js were both out-of-date. [0]
#
# XXX: Ideally we'd scope the dependency on $(UI_PROTOS*) to the appropriate
# protos DLLs, but Make v3.81 has a bug that causes the dependency to be ignored
# [1]. We're stuck with this workaround until Apple decides to update the
# version of Make they ship with macOS or we require a newer version of Make.
# Such a requirement would need to be strictly enforced, as the way this fails
# is extremely subtle and doesn't present until the web UI is loaded in the
# browser.
#
# [0]: https://stackoverflow.com/a/3077254/1122351
# [1]: http://savannah.gnu.org/bugs/?19108
.SECONDARY: $(UI_CCL_DLLS) $(UI_CCL_MANIFESTS) $(UI_OSS_DLLS) $(UI_OSS_MANIFESTS)

pkg/ui/dist/%.oss.dll.js pkg/ui/%.oss.manifest.json: pkg/ui/webpack.%.js pkg/ui/yarn.installed $(UI_PROTOS_OSS)
	$(NODE_RUN) -C pkg/ui $(WEBPACK) -p --config webpack.$*.js --env.dist=oss

pkg/ui/dist/%.ccl.dll.js pkg/ui/%.ccl.manifest.json: pkg/ui/webpack.%.js pkg/ui/yarn.installed $(UI_PROTOS_CCL)
	$(NODE_RUN) -C pkg/ui $(WEBPACK) -p --config webpack.$*.js --env.dist=ccl

.PHONY: ui-test
ui-test: $(UI_CCL_DLLS) $(UI_CCL_MANIFESTS)
	$(NODE_RUN) -C pkg/ui $(KARMA) start

.PHONY: ui-test-watch
ui-test-watch: $(UI_CCL_DLLS) $(UI_CCL_MANIFESTS)
	$(NODE_RUN) -C pkg/ui $(KARMA) start --no-single-run --auto-watch

.PHONY: ui-test-debug
ui-test-debug: $(UI_DLLS) $(UI_MANIFESTS)
	$(NODE_RUN) -C pkg/ui $(KARMA) start --browsers Chrome --no-single-run --debug --auto-watch

pkg/ui/distccl/bindata.go: $(UI_CCL_DLLS) $(UI_CCL_MANIFESTS) $(UI_JS_CCL) $(shell find pkg/ui/ccl -type f)
pkg/ui/distoss/bindata.go: $(UI_OSS_DLLS) $(UI_OSS_MANIFESTS) $(UI_JS_OSS)
pkg/ui/dist%/bindata.go: pkg/ui/webpack.app.js $(shell find pkg/ui/src pkg/ui/styl -type f) | bin/.bootstrap
	find pkg/ui/dist$* -mindepth 1 -not -name dist$*.go -delete
	set -e; shopt -s extglob; for dll in $(notdir $(filter %.dll.js,$^)); do \
	  ln -s ../dist/$$dll pkg/ui/dist$*/$${dll/@(.ccl|.oss)}; \
	done
	$(NODE_RUN) -C pkg/ui $(WEBPACK) --config webpack.app.js --env.dist=$*
	go-bindata -pkg dist$* -o $@ -prefix pkg/ui/dist$* pkg/ui/dist$*/...
	echo 'func init() { ui.Asset = Asset; ui.AssetDir = AssetDir; ui.AssetInfo = AssetInfo }' >> $@
	gofmt -s -w $@
	goimports -w $@

pkg/ui/yarn.opt.installed:
	$(NODE_RUN) -C pkg/ui/opt yarn install
	touch $@

.PHONY: ui-watch-secure
ui-watch-secure: override WEBPACK_DEV_SERVER_FLAGS += --https
ui-watch-secure: export TARGET ?= https://localhost:8080/

.PHONY: ui-watch
ui-watch: export TARGET ?= http://localhost:8080
ui-watch ui-watch-secure: PORT := 3000
ui-watch ui-watch-secure: $(UI_CCL_DLLS) pkg/ui/yarn.opt.installed
	cd pkg/ui && $(WEBPACK_DASHBOARD) -- $(WEBPACK_DEV_SERVER) --config webpack.app.js --env.dist=ccl --port $(PORT) --mode "development" $(WEBPACK_DEV_SERVER_FLAGS)

.PHONY: ui-clean
ui-clean: ## Remove build artifacts.
	find pkg/ui/dist* -mindepth 1 -not -name dist*.go -delete
	rm -f $(UI_PROTOS_CCL) $(UI_PROTOS_OSS)
	rm -f pkg/ui/*manifest.json

.PHONY: ui-maintainer-clean
ui-maintainer-clean: ## Like clean, but also remove installed dependencies
ui-maintainer-clean: ui-clean
	rm -rf pkg/ui/node_modules pkg/ui/yarn.installed

.SECONDARY: pkg/sql/parser/gen/sql.go.tmp
pkg/sql/parser/gen/sql.go.tmp: pkg/sql/parser/gen/sql-gen.y bin/.bootstrap
	set -euo pipefail; \
	  ret=$$(cd pkg/sql/parser/gen && goyacc -p sql -o sql.go.tmp sql-gen.y); \
	  if expr "$$ret" : ".*conflicts" >/dev/null; then \
	    echo "$$ret"; exit 1; \
	  fi

# The lex package needs to know about all tokens, because the encode
# functions and lexing predicates need to know about keywords, and
# keywords map to the token constants. Therefore, generate the
# constant tokens in the lex package primarily.
pkg/sql/lex/tokens.go: pkg/sql/parser/gen/sql.go.tmp
	(echo "// Code generated by make. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 echo; \
	 echo "package lex"; \
	 echo; \
	 grep '^const [A-Z][_A-Z0-9]* ' $^) > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@

# The lex package is now the primary source for the token constant
# definitions. Modify the code generated by goyacc here to refer to
# the definitions in the lex package.
pkg/sql/parser/sql.go: pkg/sql/parser/gen/sql.go.tmp | bin/.bootstrap
	(echo "// Code generated by goyacc. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 cat $^ | \
	 sed -E 's/^const ([A-Z][_A-Z0-9]*) =.*$$/const \1 = lex.\1/g') > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	goimports -w $@

# This modifies the grammar to:
# - improve the types used by the generated parser for non-terminals
# - expand the help rules.
#
# For types:
# Determine the types that will be migrated to union types by looking
# at the accessors of sqlSymUnion. The first step in this pipeline
# prints every return type of a sqlSymUnion accessor on a separate line.
# The next step regular expression escapes these types. The third
# (prepending) and the fourth (appending) steps build regular expressions
# for each of the types and store them in the file. (We make multiple
# regular expressions because we ran into a limit of characters for a
# single regex executed by sed.)
# Then translate the original syntax file, with the types determined
# above being replaced with the union type in their type declarations.
.SECONDARY: pkg/sql/parser/gen/sql-gen.y
pkg/sql/parser/gen/sql-gen.y: pkg/sql/parser/sql.y pkg/sql/parser/replace_help_rules.awk
	mkdir -p pkg/sql/parser/gen
	set -euo pipefail; \
	awk '/func.*sqlSymUnion/ {print $$(NF - 1)}' pkg/sql/parser/sql.y | \
	sed -e 's/[]\/$$*.^|[]/\\&/g' | \
	sed -e "s/^/s_(type|token) <(/" | \
	awk '{print $$0")>_\\1 <union> /* <\\2> */_"}' > pkg/sql/parser/gen/types_regex.tmp; \
	sed -E -f pkg/sql/parser/gen/types_regex.tmp < pkg/sql/parser/sql.y | \
	awk -f pkg/sql/parser/replace_help_rules.awk | \
	sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	rm pkg/sql/parser/gen/types_regex.tmp

pkg/sql/lex/reserved_keywords.go: pkg/sql/parser/sql.y pkg/sql/parser/reserved_keywords.awk | bin/.bootstrap
	awk -f pkg/sql/parser/reserved_keywords.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

pkg/sql/lex/keywords.go: pkg/sql/parser/sql.y pkg/sql/lex/all_keywords.go | bin/.bootstrap
	go run -tags all-keywords pkg/sql/lex/all_keywords.go < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

# This target will print unreserved_keywords which are not actually
# used in the grammar.
.PHONY: sqlparser-unused-unreserved-keywords
sqlparser-unused-unreserved-keywords: pkg/sql/parser/sql.y pkg/sql/parser/unreserved_keywords.awk
	@for kw in $$(awk -f pkg/sql/parser/unreserved_keywords.awk < $<); do \
	  if [ $$(grep -c $${kw} $<) -le 2 ]; then \
	    echo $${kw}; \
	  fi \
	done

pkg/sql/parser/helpmap_test.go: pkg/sql/parser/gen/sql-gen.y pkg/sql/parser/help_gen_test.sh | bin/.bootstrap
	@pkg/sql/parser/help_gen_test.sh < $< >$@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

pkg/sql/parser/help_messages.go: pkg/sql/parser/sql.y pkg/sql/parser/help.awk | bin/.bootstrap
	awk -f pkg/sql/parser/help.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

bin/.docgen_bnfs: bin/docgen
	docgen grammar bnf docs/generated/sql/bnf --quiet
	touch $@

bin/.docgen_functions: bin/docgen
	docgen functions docs/generated/sql --quiet
	touch $@

.PHONY: docs/generated/redact_safe.md

docs/generated/redact_safe.md:
	@(echo "The following types are considered always safe for reporting:"; echo; \
	  echo "File | Type"; echo "--|--") >$@.tmp || { rm -f $@.tmp; exit 1; }
	@git grep -n '^func \(.*\) SafeValue\(\)' | \
	  grep -v '^pkg/util/redact' | \
	  sed -E -e 's/^([^:]*):[0-9]+:func \(([^ ]* )?(.*)\) SafeValue.*$$/\1 | \`\3\`/g' >>$@.tmp || { rm -f $@.tmp; exit 1; }
	@git grep -n 'redact\.RegisterSafeType' | \
	  grep -v '^pkg/util/redact' | \
	  sed -E -e 's/^([^:]*):[0-9]+:.*redact\.RegisterSafeType\((.*)\).*/\1 | \`\2\`/g' >>$@.tmp || { rm -f $@.tmp; exit 1; }
	@mv -f $@.tmp $@

settings-doc-gen := $(if $(filter buildshort,$(MAKECMDGOALS)),$(COCKROACHSHORT),$(COCKROACH))

$(SETTINGS_DOC_PAGE): $(settings-doc-gen)
	@$(settings-doc-gen) gen settings-list --format=html > $@

.PHONY: execgen
execgen: $(EXECGEN_TARGETS) bin/execgen
	for i in $(EXECGEN_TARGETS); do echo EXECGEN $$i && ./bin/execgen -fmt=false $$i > $$i; done
	goimports -w $(EXECGEN_TARGETS)

# Add a catch-all rule for any non-existent execgen generated
# files. This prevents build errors when switching between branches
# that introduce or remove execgen generated files as these files are
# persisted in bin/%.d dependency lists (e.g. in bin/logictest.d).
%.eg.go: ;

optgen-defs := pkg/sql/opt/ops/*.opt
optgen-norm-rules := pkg/sql/opt/norm/rules/*.opt
optgen-xform-rules := pkg/sql/opt/xform/rules/*.opt

pkg/sql/opt/memo/expr.og.go: $(optgen-defs) bin/optgen
	optgen -out $@ exprs $(optgen-defs)

pkg/sql/opt/operator.og.go: $(optgen-defs) bin/optgen
	optgen -out $@ ops $(optgen-defs)

pkg/sql/opt/rule_name.og.go: $(optgen-defs) $(optgen-norm-rules) $(optgen-xform-rules) bin/optgen
	optgen -out $@ rulenames $(optgen-defs) $(optgen-norm-rules) $(optgen-xform-rules)

pkg/sql/opt/rule_name_string.go: pkg/sql/opt/rule_name.go pkg/sql/opt/rule_name.og.go bin/.bootstrap
	stringer -output=$@ -type=RuleName $(filter %.go,$^)

pkg/sql/opt/xform/explorer.og.go: $(optgen-defs) $(optgen-xform-rules) bin/optgen
	optgen -out $@ explorer $(optgen-defs) $(optgen-xform-rules)

pkg/sql/opt/norm/factory.og.go: $(optgen-defs) $(optgen-norm-rules) bin/optgen
	optgen -out $@ factory $(optgen-defs) $(optgen-norm-rules)

# Format non-generated .cc and .h files in libroach using clang-format.
.PHONY: c-deps-fmt
c-deps-fmt:
	find $(LIBROACH_SRC_DIR) -name '*.cc' -o -name '*.h' | xargs grep -L 'DO NOT EDIT' | xargs clang-format -i

.PHONY: clean-c-deps
clean-c-deps:
	rm -rf $(CRYPTOPP_DIR)
	rm -rf $(JEMALLOC_DIR)
	rm -rf $(PROTOBUF_DIR)
	rm -rf $(ROCKSDB_DIR)
	rm -rf $(SNAPPY_DIR)
	rm -rf $(GEOS_DIR)
	rm -rf $(PROJ_DIR)
	rm -rf $(LIBROACH_DIR)
	rm -rf $(KRB5_DIR)

.PHONY: unsafe-clean-c-deps
unsafe-clean-c-deps:
	git -C $(CRYPTOPP_SRC_DIR) clean -dxf
	git -C $(JEMALLOC_SRC_DIR) clean -dxf
	git -C $(PROTOBUF_SRC_DIR) clean -dxf
	git -C $(ROCKSDB_SRC_DIR)  clean -dxf
	git -C $(SNAPPY_SRC_DIR)   clean -dxf
	git -C $(GEOS_SRC_DIR)     clean -dxf
	git -C $(PROJ_SRC_DIR)     clean -dxf
	git -C $(LIBROACH_SRC_DIR) clean -dxf
	git -C $(KRB5_SRC_DIR)     clean -dxf

.PHONY: clean
clean: ## Remove build artifacts.
clean: clean-c-deps
	rm -rf bin/.go_protobuf_sources bin/.gw_protobuf_sources bin/.cpp_protobuf_sources build/defs.mk*
	-$(GO) clean $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i -cache github.com/cockroachdb/cockroach...
	$(FIND_RELEVANT) -type f \( -name 'zcgo_flags*.go' -o -name '*.test' \) -exec rm {} +
	for f in cockroach*; do if [ -f "$$f" ]; then rm "$$f"; fi; done
	rm -rf artifacts bin $(ARCHIVE) pkg/sql/parser/gen

.PHONY: maintainer-clean
maintainer-clean: ## Like clean, but also remove some auto-generated source code.
maintainer-clean: clean ui-maintainer-clean
	rm -f $(SQLPARSER_TARGETS) $(OPTGEN_TARGETS) $(UI_PROTOS_OSS) $(UI_PROTOS_CCL)

.PHONY: unsafe-clean
unsafe-clean: ## Like maintainer-clean, but also remove ALL untracked/ignored files.
unsafe-clean: maintainer-clean unsafe-clean-c-deps
	git clean -dxf

# The following rules automatically generate dependency information for Go
# binaries. See [0] for details on the approach.
#
# [0]: http://make.mad-scientist.net/papers/advanced-auto-dependency-generation/

bins = \
  bin/allocsim \
  bin/benchmark \
  bin/cockroach-oss \
  bin/cockroach-short \
  bin/compile-builds \
  bin/docgen \
  bin/execgen \
  bin/fuzz \
  bin/generate-binary \
  bin/terraformgen \
  bin/github-post \
  bin/github-pull-request-make \
  bin/gossipsim \
  bin/langgen \
  bin/protoc-gen-gogoroach \
  bin/publish-artifacts \
  bin/publish-provisional-artifacts \
  bin/optfmt \
  bin/optgen \
  bin/returncheck \
  bin/roachvet \
  bin/roachprod \
  bin/roachprod-stress \
  bin/roachtest \
  bin/teamcity-trigger \
  bin/uptodate \
  bin/urlcheck \
  bin/workload \
  bin/zerosum

testbins = \
  bin/logictest \
  bin/logictestopt \
  bin/logictestccl

# Mappings for binaries that don't live in pkg/cmd.
execgen-package = ./pkg/sql/colexec/execgen/cmd/execgen
langgen-package = ./pkg/sql/opt/optgen/cmd/langgen
optfmt-package = ./pkg/sql/opt/optgen/cmd/optfmt
optgen-package = ./pkg/sql/opt/optgen/cmd/optgen
logictest-package = ./pkg/sql/logictest
logictestccl-package = ./pkg/ccl/logictestccl
logictestopt-package = ./pkg/sql/opt/exec/execbuilder
terraformgen-package = ./pkg/cmd/roachprod/vm/aws/terraformgen
logictest-bins := bin/logictest bin/logictestopt bin/logictestccl

# Additional dependencies for binaries that depend on generated code.
#
# TODO(benesch): Derive this automatically. This is getting out of hand.
bin/workload bin/docgen bin/execgen bin/roachtest $(logictest-bins): $(SQLPARSER_TARGETS) $(PROTOBUF_TARGETS)
bin/workload bin/docgen bin/roachtest $(logictest-bins): $(LIBPROJ) $(CGO_FLAGS_FILES)
bin/roachtest $(logictest-bins): $(C_LIBS_CCL) $(CGO_FLAGS_FILES) $(OPTGEN_TARGETS)

$(bins): bin/%: bin/%.d | bin/prereqs bin/.submodules-initialized
	@echo go install -v $*
	bin/prereqs $(if $($*-package),$($*-package),./pkg/cmd/$*) > $@.d.tmp
	mv -f $@.d.tmp $@.d
	@$(GO_INSTALL) -v $(if $($*-package),$($*-package),./pkg/cmd/$*)

$(testbins): bin/%: bin/%.d | bin/prereqs $(SUBMODULES_TARGET)
	@echo go test -c $($*-package)
	bin/prereqs -bin-name=$* -test $($*-package) > $@.d.tmp
	mv -f $@.d.tmp $@.d
	$(xgo) test $(GOTESTFLAGS) $(GOFLAGS) $(GOMODVENDORFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o $@ $($*-package)

bin/prereqs: ./pkg/cmd/prereqs/*.go | bin/.submodules-initialized
	@echo go install -v ./pkg/cmd/prereqs
	@$(GO_INSTALL) -v ./pkg/cmd/prereqs

.PHONY: fuzz
fuzz: ## Run fuzz tests.
fuzz: bin/fuzz
	bin/fuzz $(TESTFLAGS) -tests $(TESTS) -timeout $(TESTTIMEOUT) $(PKG)


# No need to include all the dependency files if the user is just
# requesting help or cleanup.
ifneq ($(build-with-dep-files),)
.SECONDARY: bin/%.d
.PRECIOUS: bin/%.d
bin/%.d: ;

include $(wildcard bin/*.d)
endif

# Make doesn't expose a list of the variables declared in a given file, so we
# resort to sed magic. Roughly, this sed command prints VARIABLE in lines of the
# following forms:
#
#     [export] VARIABLE [:+?]=
#     TARGET-NAME: [export] VARIABLE [:+?]=
#
# The additional complexity below handles whitespace and comments.
#
# The special comments at the beginning are for Github/Go/Reviewable:
# https://github.com/golang/go/issues/13560#issuecomment-277804473
# https://github.com/Reviewable/Reviewable/wiki/FAQ#how-do-i-tell-reviewable-that-a-file-is-generated-and-should-not-be-reviewed
# Note how the 'prefix' variable is manually appended. This is required by Homebrew.
.SECONDARY: build/variables.mk
build/variables.mk: Makefile build/archive/contents/Makefile pkg/ui/Makefile build/defs.mk
	@echo '# Code generated by Make. DO NOT EDIT.' > $@.tmp
	@echo '# GENERATED FILE DO NOT EDIT' >> $@.tmp
	@echo 'define VALID_VARS' >> $@.tmp
	@sed -nE -e '/^	/d' -e 's/([^#]*)#.*/\1/' \
	  -e 's/(^|^[^:]+:)[ ]*(export)?[ ]*([[:upper:]_]+)[ ]*[:?+]?=.*/  \3/p' $^ \
	  | sort -u >> $@.tmp
	@echo '  prefix' >> $@.tmp
	@echo 'endef' >> $@.tmp
	@set -e; \
	if ! cmp -s $@.tmp $@; then \
	   mv -f $@.tmp $@; \
	else rm -f $@.tmp; fi

# Print an error if the user specified any variables on the command line that
# don't appear in this Makefile. The list of valid variables is automatically
# rebuilt on the first successful `make` invocation after the Makefile changes.
#
# TODO(peter): Figure out how to disallow overriding of variables that
# are not in the valid list from the environment. The problem is that
# any environment variable becomes a make variable and environments
# are dirty. For instance, my includes GREP_COLOR.
include build/variables.mk
$(foreach v,$(filter-out $(strip $(VALID_VARS)),$(.VARIABLES)),\
	$(if $(findstring command line,$(origin $v)),$(error Variable '$v' is not recognized by this Makefile)))
