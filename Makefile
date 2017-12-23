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

## Which package to run tests against, e.g. "./pkg/storage".
PKG := ./pkg/...

## Tests to run for use with `make test`.
TESTS := .

## Benchmarks to run for use with `make bench`.
BENCHES :=

## Space delimited list of logic test files to run, for make testlogic and testccllogic.
FILES :=

## Regex for matching logic test subtests. This is always matched after "FILES"
## if they are provided.
SUBTESTS :=

## Test timeout to use for regular tests.
TESTTIMEOUT := 4m

## Test timeout to use for race tests.
RACETIMEOUT := 25m

## Test timeout to use for acceptance tests.
ACCEPTANCETIMEOUT := 30m

## Test timeout to use for benchmarks.
BENCHTIMEOUT := 5m

## Extra flags to pass to the go test runner, e.g. "-v --vmodule=raft=1"
TESTFLAGS :=

## Extra flags to pass to `stress` during `make stress`.
STRESSFLAGS :=

DUPLFLAGS    := -t 100
GOFLAGS      :=
TAGS         :=
ARCHIVE      := cockroach.src.tgz
STARTFLAGS   := -s type=mem,size=1GiB --logtostderr
BUILDMODE    := install
BUILDTARGET  := .
SUFFIX       :=
INSTALL      := install
prefix       := /usr/local
bindir       := $(prefix)/bin

MAKEFLAGS += $(shell build/jflag.sh)

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
		"make testlogic" "Run all SQL Logic Tests." \
		"make testlogic FILES=prepare" "Run the logic test with filename prepare." \
		"make testlogic FILES=fk SUBTESTS='(20042|20045)'" "Run the logic test with filename fk and only subtests 20042 and 20045." \
		"make testccllogic" "Run all SQL CCL Logic Tests."

# Possible values:
# <empty>: use the default toolchain
# release-linux-gnu:  target Linux 2.6.32, dynamically link GLIBC 2.12.2
# release-linux-musl: target Linux 2.6.32, statically link musl 1.1.16
# release-darwin:     target OS X 10.9
# release-windows:    target Windows 8, statically link all non-Windows libraries
#
# All non-empty variants only work in the cockroachdb/builder docker image, as
# they depend on cross-compilation toolchains available there.
# The name of the cockroach binary depends on the release type.
TYPE :=

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
LINKFLAGS ?=

BUILD_TYPE := development
ifeq ($(TYPE),)
else ifeq ($(TYPE),msan)
NATIVE_SUFFIX := _msan
override GOFLAGS += -msan
# NB: using jemalloc with msan causes segfaults. See
# https://github.com/jemalloc/jemalloc/issues/821.
override TAGS += stdmalloc
MSAN_CPPFLAGS := -fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -I/libcxx_msan/include -I/libcxx_msan/include/c++/v1
MSAN_LDFLAGS  := -fsanitize=memory -stdlib=libc++ -L/libcxx_msan/lib -lc++abi -Wl,-rpath,/libcxx_msan/lib
override CGO_CPPFLAGS += $(MSAN_CPPFLAGS)
override CGO_LDFLAGS += $(MSAN_LDFLAGS)
export CGO_CPPFLAGS
export CGO_LDFLAGS
# NB: CMake doesn't respect CPPFLAGS (!)
#
# See https://bugs.launchpad.net/pantheon-terminal/+bug/1325329.
override CFLAGS += $(MSAN_CPPFLAGS)
override CXXFLAGS += $(MSAN_CPPFLAGS)
override LDFLAGS += $(MSAN_LDFLAGS)
export CFLAGS
export CXXFLAGS
export LDFLAGS
else ifeq ($(TYPE),portable)
override LINKFLAGS += -s -w -extldflags "-static-libgcc -static-libstdc++"
else ifeq ($(TYPE),release-linux-gnu)
# We use a custom toolchain to target old Linux and glibc versions. However,
# this toolchain's libstdc++ version is quite recent and must be statically
# linked to avoid depending on the target's available libstdc++.
XHOST_TRIPLE := x86_64-unknown-linux-gnu
override LINKFLAGS += -s -w -extldflags "-static-libgcc -static-libstdc++"
override GOFLAGS += -installsuffix release-gnu
override SUFFIX := $(SUFFIX)-linux-2.6.32-gnu-amd64
BUILD_TYPE := release-gnu
else ifeq ($(TYPE),release-linux-musl)
BUILD_TYPE := release-musl
XHOST_TRIPLE := x86_64-unknown-linux-musl
override LINKFLAGS += -s -w -extldflags "-static"
override GOFLAGS += -installsuffix release-musl
override SUFFIX := $(SUFFIX)-linux-2.6.32-musl-amd64
else ifeq ($(TYPE),release-darwin)
XGOOS := darwin
export CGO_ENABLED := 1
XHOST_TRIPLE := x86_64-apple-darwin13
override SUFFIX := $(SUFFIX)-darwin-10.9-amd64
override LINKFLAGS += -s -w
BUILD_TYPE := release
else ifeq ($(TYPE),release-windows)
XGOOS := windows
export CGO_ENABLED := 1
XHOST_TRIPLE := x86_64-w64-mingw32
override SUFFIX := $(SUFFIX)-windows-6.2-amd64
override LINKFLAGS += -s -w -extldflags "-static"
BUILD_TYPE := release
else
$(error unknown build type $(TYPE))
endif

override LINKFLAGS += -X github.com/cockroachdb/cockroach/pkg/build.typ=$(BUILD_TYPE)

GO      ?= go
GOFLAGS ?=
XGO     ?= xgo
TAR     ?= tar

# Convenience variables for important paths.
PKG_ROOT       := ./pkg
UI_ROOT        := $(PKG_ROOT)/ui
SQLPARSER_ROOT := $(PKG_ROOT)/sql/parser

# Ensure we have an unambiguous GOPATH.
export GOPATH := $(realpath ../../../..)
#                           ^  ^  ^  ^~ GOPATH
#                           |  |  |~ GOPATH/src
#                           |  |~ GOPATH/src/github.com
#                           |~ GOPATH/src/github.com/cockroachdb

# Avoid printing twice if Make restarts (because a Makefile was changed) or is
# called recursively from another Makefile.
ifeq ($(MAKE_RESTARTS)$(MAKELEVEL),0)
$(info GOPATH set to $(GOPATH))
endif

# We install our vendored tools to a directory within this repository to avoid
# overwriting any user-installed binaries of the same name in the default GOBIN.
LOCAL_BIN := $(abspath bin)
GO_INSTALL := GOBIN='$(LOCAL_BIN)' $(GO) install

# Prefer tools we've installed with go install and Yarn to those elsewhere on
# the PATH.
export PATH := $(LOCAL_BIN):$(PATH)

# HACK: Make has a fast path and a slow path for command execution,
# but the fast path uses the PATH variable from when make was started,
# not the one we set on the previous line. In order for the above
# line to have any effect, we must force make to always take the slow path.
# Setting the SHELL variable to a value other than the default (/bin/sh)
# is one way to do this globally.
# http://stackoverflow.com/questions/8941110/how-i-could-add-dir-to-path-in-makefile/13468229#13468229
export SHELL := $(shell which bash)
ifeq ($(SHELL),)
$(error bash is required)
endif

GIT_DIR := $(shell git rev-parse --git-dir 2> /dev/null)

# Invocation of any NodeJS script should be prefixed by NODE_RUN. See the
# comments within node-run.sh for rationale.
NODE_RUN := build/node-run.sh

# make-lazy converts a recursive variable, which is evaluated every time it's
# referenced, to a lazy variable, which is evaluated only the first time it's
# used. See: http://blog.jgc.org/2016/07/lazy-gnu-make-variables.html
override make-lazy = $(eval $1 = $$(eval $1 := $(value $1))$$($1))

UNAME := $(shell uname)
MACOS := $(findstring Darwin,$(UNAME))
MINGW := $(findstring MINGW,$(UNAME))

# GNU tar and BSD tar both support transforming filenames according to a regular
# expression, but have different flags to do so.
TAR_XFORM_FLAG = $(shell $(TAR) --version | grep -q GNU && echo "--xform='flags=r;s'" || echo "-s")
$(call make-lazy,TAR_XFORM_FLAG)

# To edit in-place without creating a backup file, GNU sed requires a bare -i,
# while BSD sed requires an empty string as the following argument.
SED_INPLACE = sed $(shell sed --version 2>&1 | grep -q GNU && echo -i || echo "-i ''")
$(call make-lazy,SED_INPLACE)

# This is how you get a literal space into a Makefile.
space := $(eval) $(eval)

# Color support.
yellow = $(shell tput setaf 3 2>/dev/null)
cyan = $(shell tput setaf 6 2>/dev/null)
term-reset = $(shell tput sgr0)
$(call make-lazy,yellow)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)

# We used to check the Go version in a .PHONY .go-version target, but the error
# message, if any, would get mixed in with noise from other targets if Make was
# executed in parallel job mode. This check, by contrast, is guaranteed to print
# its error message before any noisy output.
#
# Note that word boundary markers (\b, \<, [[:<:]]) are not portable, but `grep
# -w`, though not required by POSIX, exists on all tested platforms.
GOVERS := go1\.9.*
ifeq ($(shell $(GO) version | grep -qwE '$(GOVERS)' && echo y),)
$(error "$(GOVERS) required (see CONTRIBUTING.md): $(shell $(GO) version); use `make GOVERS=.*` for experiments")
endif

# Print an error if the user specified any variables on the command line that
# don't appear in this Makefile. The list of valid variables is automatically
# rebuilt on the first successful `make` invocation after the Makefile changes.
include build/variables.mk
$(foreach v,$(filter-out $(strip $(VALID_VARS)),$(.VARIABLES)),\
	$(if $(findstring command line,$(origin $v)),$(error Variable '$v' is not recognized by this Makefile)))
-include customenv.mk

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
GITHOOKSDIR := $(shell test -d .git && echo '.git/hooks' || git rev-parse --git-path hooks)
GITHOOKS := $(subst githooks/,$(GITHOOKSDIR)/,$(wildcard githooks/*))
$(GITHOOKSDIR)/%: githooks/%
	@echo installing $<
	@rm -f $@
	@mkdir -p $(dir $@)
	@ln -s ../../$(basename $<) $(dir $@)
endif

# Make does textual matching on target names, so e.g. yarn.installed and
# ../../pkg/ui/yarn.installed are considered different targets even when the CWD
# is pkg/ui. Introducing a variable for targets that are used across Makefiles
# with different CWDs decreases the chance of accidentally using the wrong path
# to a target.
YARN_INSTALLED_TARGET := $(UI_ROOT)/yarn.installed

.SECONDARY: $(YARN_INSTALLED_TARGET)
$(YARN_INSTALLED_TARGET): $(BOOTSTRAP_TARGET) $(UI_ROOT)/package.json $(UI_ROOT)/yarn.lock
	$(NODE_RUN) -C $(UI_ROOT) yarn install
	# Prevent ProtobufJS from trying to install its own packages because a) the
	# the feature is buggy, and b) it introduces an unnecessary dependency on NPM.
	# Additionally pin a known-good version of jsdoc.
	# See: https://github.com/dcodeIO/protobuf.js/issues/716.
	cp $(UI_ROOT)/node_modules/protobufjs/cli/{package.standalone.json,package.json}
	$(NODE_RUN) -C $(UI_ROOT)/node_modules/protobufjs/cli yarn add jsdoc@3.4.3
	$(NODE_RUN) -C $(UI_ROOT)/node_modules/protobufjs/cli yarn install
	@# We remove this broken dependency again in pkg/ui/webpack.config.js.
	@# See the comment there for details.
	rm -rf $(UI_ROOT)/node_modules/@types/node
	touch $@

# We store the bootstrap marker file in the bin directory so that remapping bin,
# like we do in the builder container to allow for different host and guest
# systems, will trigger bootstrapping in the container as necessary. This is
# extracted into a variable for the same reasons as YARN_INSTALLED_TARGET.
BOOTSTRAP_TARGET := $(LOCAL_BIN)/.bootstrap

SUBMODULES_TARGET := $(LOCAL_BIN)/.submodules-initialized

# Update the git hooks and install commands from dependencies whenever they
# change.
$(BOOTSTRAP_TARGET): $(GITHOOKS) Gopkg.lock $(LOCAL_BIN)/returncheck | $(SUBMODULES_TARGET)
	@$(GO_INSTALL) -v \
		./vendor/github.com/golang/dep/cmd/dep \
		./vendor/github.com/client9/misspell/cmd/misspell \
		./vendor/github.com/cockroachdb/crlfmt \
		./vendor/github.com/cockroachdb/stress \
		./vendor/github.com/golang/lint/golint \
		./vendor/github.com/google/pprof \
		./vendor/github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway \
		./vendor/github.com/jteeuwen/go-bindata/go-bindata \
		./vendor/github.com/kisielk/errcheck \
		./vendor/github.com/mattn/goveralls \
		./vendor/github.com/mibk/dupl \
		./vendor/github.com/wadey/gocovmerge \
		./vendor/golang.org/x/perf/cmd/benchstat \
		./vendor/golang.org/x/tools/cmd/goimports \
		./vendor/golang.org/x/tools/cmd/goyacc \
		./vendor/golang.org/x/tools/cmd/stringer
	touch $@

$(SUBMODULES_TARGET):
ifneq ($(GIT_DIR),)
	git submodule update --init
endif
	mkdir -p $(@D)
	touch $@

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
build/variables.mk: Makefile build/archive/contents/Makefile $(UI_ROOT)/Makefile
	@echo '# Code generated by Make. DO NOT EDIT.' > $@
	@echo '# GENERATED FILE DO NOT EDIT' >> $@
	@echo 'define VALID_VARS' >> $@
	@sed -nE -e '/^	/d' -e 's/([^#]*)#.*/\1/' \
	  -e 's/(^|^[^:]+:)[ ]*(export)?[ ]*([^ ]+)[ ]*[:?+]?=.*/  \3/p' $^ \
	  | sort -u >> $@
	@echo 'endef' >> $@

# The following section handles building our C/C++ dependencies. These are
# common because both the root Makefile and protobuf.mk have C dependencies.

C_DEPS_DIR := $(abspath c-deps)
CRYPTOPP_SRC_DIR := $(C_DEPS_DIR)/cryptopp
JEMALLOC_SRC_DIR := $(C_DEPS_DIR)/jemalloc
PROTOBUF_SRC_DIR := $(C_DEPS_DIR)/protobuf
ROCKSDB_SRC_DIR  := $(C_DEPS_DIR)/rocksdb
SNAPPY_SRC_DIR   := $(C_DEPS_DIR)/snappy
LIBROACH_SRC_DIR := $(C_DEPS_DIR)/libroach

HOST_TRIPLE := $(shell $$($(GO) env CC) -dumpmachine)

CONFIGURE_FLAGS :=
CMAKE_FLAGS := $(if $(MINGW),-G 'MSYS Makefiles')

# override so that no one is tempted to make USE_STDMALLOC=1 instead of make
# TAGS=stdmalloc; without TAGS=stdmalloc, Go will still try to link jemalloc.
override USE_STDMALLOC := $(findstring stdmalloc,$(TAGS))
STDMALLOC_SUFFIX := $(if $(USE_STDMALLOC),_stdmalloc)

# TODO(benesch): Give TYPE clearer semantics to avoid this spaghetti.
PORTABLE := $(or $(findstring portable,$(TYPE)),$(findstring release,$(TYPE)))

ENABLE_ROCKSDB_ASSERTIONS := $(findstring race,$(TAGS))

ifdef XHOST_TRIPLE

# Darwin wants clang, so special treatment is in order.
ISDARWIN := $(findstring darwin,$(XHOST_TRIPLE))

XHOST_BIN_DIR := /x-tools/$(XHOST_TRIPLE)/bin

export PATH := $(XHOST_BIN_DIR):$(PATH)

CC_PATH  := $(XHOST_BIN_DIR)/$(XHOST_TRIPLE)
CXX_PATH := $(XHOST_BIN_DIR)/$(XHOST_TRIPLE)
ifdef ISDARWIN
CC_PATH  := $(CC_PATH)-clang
CXX_PATH := $(CXX_PATH)-clang++
else
CC_PATH  := $(CC_PATH)-gcc
CXX_PATH := $(CXX_PATH)-g++
endif

ifdef ISDARWIN
CMAKE_SYSTEM_NAME := Darwin
else ifneq ($(findstring linux,$(XHOST_TRIPLE)),)
CMAKE_SYSTEM_NAME := Linux
else ifneq ($(findstring mingw,$(XHOST_TRIPLE)),)
CMAKE_SYSTEM_NAME := Windows
endif

CONFIGURE_FLAGS += --host=$(XHOST_TRIPLE) CC=$(CC_PATH) CXX=$(CXX_PATH)
CMAKE_FLAGS += -DCMAKE_C_COMPILER=$(CC_PATH) -DCMAKE_CXX_COMPILER=$(CXX_PATH) -DCMAKE_SYSTEM_NAME=$(CMAKE_SYSTEM_NAME)

TARGET_TRIPLE := $(XHOST_TRIPLE)
else
TARGET_TRIPLE := $(HOST_TRIPLE)
endif

NATIVE_SPECIFIER := $(TARGET_TRIPLE)$(NATIVE_SUFFIX)
BUILD_DIR := $(GOPATH)/native/$(NATIVE_SPECIFIER)

# In MinGW, cgo flags don't handle Unix-style paths, so convert our base path to
# a Windows-style path.
#
# TODO(benesch): Figure out why. MinGW transparently converts Unix-style paths
# everywhere else.
ifdef MINGW
BUILD_DIR := $(shell cygpath -m $(BUILD_DIR))
endif

CRYPTOPP_DIR := $(BUILD_DIR)/cryptopp
JEMALLOC_DIR := $(BUILD_DIR)/jemalloc
PROTOBUF_DIR := $(BUILD_DIR)/protobuf
ROCKSDB_DIR  := $(BUILD_DIR)/rocksdb$(STDMALLOC_SUFFIX)$(if $(ENABLE_ROCKSDB_ASSERTIONS),_assert)
SNAPPY_DIR   := $(BUILD_DIR)/snappy
LIBROACH_DIR := $(BUILD_DIR)/libroach
# Can't share with protobuf because protoc is always built for the host.
PROTOC_DIR := $(GOPATH)/native/$(HOST_TRIPLE)/protobuf
PROTOC 		 := $(PROTOC_DIR)/protoc

C_LIBS_OSS = $(if $(USE_STDMALLOC),,libjemalloc) libprotobuf libsnappy librocksdb libroach
C_LIBS_CCL = $(C_LIBS_OSS) libcryptopp libroachccl

# Go does not permit dashes in build tags. This is undocumented. Fun!
NATIVE_SPECIFIER_TAG := $(subst -,_,$(NATIVE_SPECIFIER))$(STDMALLOC_SUFFIX)

# In each package that uses cgo, we inject include and library search paths
# into files named zcgo_flags[_arch_vendor_os_abi].go. The logic for this is
# complicated so that Make-driven builds can cache the state of builds for
# multiple architectures at once, while still allowing the use of `go build`
# and `go test` for the architecture most recently built with Make.
#
# Building with Make always adds the `make` and `arch_vendor_os_abi` tags to
# the build.
#
# Unsuffixed flags files (zcgo_flags.cgo) have the build constraint `!make`
# and are only compiled when invoking the Go toolchain directly on a package--
# i.e., when the `make` build tag is not specified. These files are rebuilt on
# every Make invocation, and so reflect the target triple that Make was most
# recently invoked with.
#
# Suffixed flags files (e.g. zcgo_flags_arch_vendor_os_abi.go) have the build
# constraint `arch_vendor_os_abi` and are built the first time a Make-driven
# build encounters a given `arch_vendor_os_abi` target triple. The Go
# toolchain does not automatically set target-triple build tags, so these
# files are only compiled when building with Make.
CGO_PKGS := cli server/status storage/engine ccl/storageccl/engineccl
CGO_UNSUFFIXED_FLAGS_FILES := $(addprefix $(PKG_ROOT)/,$(addsuffix /zcgo_flags.go,$(CGO_PKGS)))
CGO_SUFFIXED_FLAGS_FILES   := $(addprefix $(PKG_ROOT)/,$(addsuffix /zcgo_flags_$(NATIVE_SPECIFIER_TAG).go,$(CGO_PKGS)))
CGO_FLAGS_FILES := $(CGO_UNSUFFIXED_FLAGS_FILES) $(CGO_SUFFIXED_FLAGS_FILES)

$(CGO_UNSUFFIXED_FLAGS_FILES): .ALWAYS_REBUILD

$(CGO_FLAGS_FILES): Makefile
	@echo 'GEN $@'
	@echo '// GENERATED FILE DO NOT EDIT' > $@
	@echo >> $@
	@echo '// +build $(if $(findstring $(NATIVE_SPECIFIER_TAG),$@),$(NATIVE_SPECIFIER_TAG),!make)' >> $@
	@echo >> $@
	@echo 'package $(notdir $(@D))' >> $@
	@echo >> $@
	@echo '// #cgo CPPFLAGS: -I$(JEMALLOC_DIR)/include' >> $@
	@echo '// #cgo LDFLAGS: $(addprefix -L,$(CRYPTOPP_DIR) $(PROTOBUF_DIR) $(JEMALLOC_DIR)/lib $(SNAPPY_DIR) $(ROCKSDB_DIR) $(LIBROACH_DIR))' >> $@
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
$(CRYPTOPP_DIR)/Makefile: $(C_DEPS_DIR)/cryptopp-rebuild $(BOOTSTRAP_TARGET)
	rm -rf $(CRYPTOPP_DIR)
	mkdir -p $(CRYPTOPP_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/cryptopp-rebuild. See above for rationale.
	cd $(CRYPTOPP_DIR) && cmake $(CMAKE_FLAGS) $(CRYPTOPP_SRC_DIR)

$(JEMALLOC_SRC_DIR)/configure.ac: $(BOOTSTRAP_TARGET)

$(JEMALLOC_SRC_DIR)/configure: $(JEMALLOC_SRC_DIR)/configure.ac
	cd $(JEMALLOC_SRC_DIR) && autoconf

$(JEMALLOC_DIR)/Makefile: $(C_DEPS_DIR)/jemalloc-rebuild $(JEMALLOC_SRC_DIR)/configure
	rm -rf $(JEMALLOC_DIR)
	mkdir -p $(JEMALLOC_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/jemalloc-rebuild. See above for rationale.
	@#
	@# jemalloc profiling deadlocks when built against musl. See
	@# https://github.com/jemalloc/jemalloc/issues/585.
	cd $(JEMALLOC_DIR) && $(JEMALLOC_SRC_DIR)/configure $(CONFIGURE_FLAGS) $(if $(findstring musl,$(TARGET_TRIPLE)),,--enable-prof)

$(PROTOBUF_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild $(BOOTSTRAP_TARGET)
	rm -rf $(PROTOBUF_DIR)
	mkdir -p $(PROTOBUF_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOBUF_DIR) && cmake $(CMAKE_FLAGS) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake

ifneq ($(PROTOC_DIR),$(PROTOBUF_DIR))
$(PROTOC_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild $(BOOTSTRAP_TARGET)
	rm -rf $(PROTOC_DIR)
	mkdir -p $(PROTOC_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOC_DIR) && cmake $(CMAKE_FLAGS) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake
endif

$(ROCKSDB_DIR)/Makefile: $(C_DEPS_DIR)/rocksdb-rebuild $(BOOTSTRAP_TARGET) | libsnappy $(if $(USE_STDMALLOC),,libjemalloc)
	rm -rf $(ROCKSDB_DIR)
	mkdir -p $(ROCKSDB_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/rocksdb-rebuild. See above for rationale.
	cd $(ROCKSDB_DIR) && cmake $(CMAKE_FLAGS) $(ROCKSDB_SRC_DIR) \
	  $(if $(PORTABLE),-DPORTABLE=ON) \
	  -DSNAPPY_LIBRARIES=$(SNAPPY_DIR)/libsnappy.a -DSNAPPY_INCLUDE_DIR="$(SNAPPY_SRC_DIR);$(SNAPPY_DIR)" -DWITH_SNAPPY=ON \
	  $(if $(USE_STDMALLOC),,-DJEMALLOC_LIBRARIES=$(JEMALLOC_DIR)/lib/libjemalloc.a -DJEMALLOC_INCLUDE_DIR=$(JEMALLOC_DIR)/include -DWITH_JEMALLOC=ON) \
	  -DCMAKE_CXX_FLAGS="$(if $(findstring x86_64,$(TARGET_TRIPLE)),-msse3) $(if $(ENABLE_ROCKSDB_ASSERTIONS),,-DNDEBUG)"
	@# TODO(benesch): Tweak how we pass -DNDEBUG above when we upgrade to a
	@# RocksDB release that includes https://github.com/facebook/rocksdb/pull/2300.

$(SNAPPY_DIR)/Makefile: $(C_DEPS_DIR)/snappy-rebuild $(BOOTSTRAP_TARGET)
	rm -rf $(SNAPPY_DIR)
	mkdir -p $(SNAPPY_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/snappy-rebuild. See above for rationale.
	cd $(SNAPPY_DIR) && cmake $(CMAKE_FLAGS) $(SNAPPY_SRC_DIR)

$(LIBROACH_DIR)/Makefile: $(C_DEPS_DIR)/libroach-rebuild $(BOOTSTRAP_TARGET)
	rm -rf $(LIBROACH_DIR)
	mkdir -p $(LIBROACH_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/libroach-rebuild. See above for rationale.
	cd $(LIBROACH_DIR) && cmake $(CMAKE_FLAGS) $(LIBROACH_SRC_DIR) -DCMAKE_BUILD_TYPE=Release \
		-DPROTOBUF_LIB=$(PROTOBUF_DIR)/libprotobuf.a -DROCKSDB_LIB=$(ROCKSDB_DIR)/librocksdb.a \
		-DJEMALLOC_LIB=$(JEMALLOC_DIR)/lib/libjemalloc.a -DSNAPPY_LIB=$(SNAPPY_DIR)/libsnappy.a \
		-DCRYPTOPP_LIB=$(CRYPTOPP_DIR)/libcryptopp.a

# We mark C and C++ dependencies as .PHONY (or .ALWAYS_REBUILD) to avoid
# having to name the artifact (for .PHONY), which can vary by platform, and so
# the child Makefile can determine whether the target is up to date (for both
# .PHONY and .ALWAYS_REBUILD). We don't have the targets' prerequisites here,
# and we certainly don't want to duplicate them.

$(PROTOC): $(PROTOC_DIR)/Makefile .ALWAYS_REBUILD | libprotobuf
	@$(MAKE) --no-print-directory -C $(PROTOC_DIR) protoc

.PHONY: libcryptopp
libcryptopp: $(CRYPTOPP_DIR)/Makefile
	@$(MAKE) --no-print-directory -C $(CRYPTOPP_DIR) static

.PHONY: libjemalloc
libjemalloc: $(JEMALLOC_DIR)/Makefile
	@$(MAKE) --no-print-directory -C $(JEMALLOC_DIR) build_lib_static

.PHONY: libprotobuf
libprotobuf: $(PROTOBUF_DIR)/Makefile
	@$(MAKE) --no-print-directory -C $(PROTOBUF_DIR) libprotobuf

.PHONY: libsnappy
libsnappy: $(SNAPPY_DIR)/Makefile
	@$(MAKE) --no-print-directory -C $(SNAPPY_DIR) snappy

.PHONY: librocksdb
librocksdb: $(ROCKSDB_DIR)/Makefile
	@$(MAKE) --no-print-directory -C $(ROCKSDB_DIR) rocksdb

.PHONY: libroach
libroach: $(LIBROACH_DIR)/Makefile $(CPP_PROTOS_TARGET)
	@$(MAKE) --no-print-directory -C $(LIBROACH_DIR) roach

.PHONY: libroachccl
libroachccl: $(LIBROACH_DIR)/Makefile $(CPP_PROTOS_CCL_TARGET) libroach
	@$(MAKE) --no-print-directory -C $(LIBROACH_DIR) roachccl

PHONY: check-libroach
check-libroach: $(LIBROACH_DIR)/Makefile libjemalloc libprotobuf libsnappy librocksdb libcryptopp
	@$(MAKE) --no-print-directory -C $(LIBROACH_DIR) check

override TAGS += make $(NATIVE_SPECIFIER_TAG)

# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
ifdef MACOS
export MACOSX_DEPLOYMENT_TARGET ?= $(shell sw_vers -productVersion | grep -oE '[0-9]+\.[0-9]+')
endif

# Some targets (protobuf) produce different results depending on the sort order;
# set LC_COLLATE so this is consistent across systems.
export LC_COLLATE=C

XGO := $(strip $(if $(XGOOS),GOOS=$(XGOOS)) $(if $(XGOARCH),GOARCH=$(XGOARCH)) $(if $(XHOST_TRIPLE),CC=$(CC_PATH) CXX=$(CXX_PATH)) $(GO))

COCKROACH := ./cockroach$(SUFFIX)$(shell $(XGO) env GOEXE)

SQLPARSER_TARGETS = \
	$(SQLPARSER_ROOT)/sql.go \
	$(SQLPARSER_ROOT)/helpmap_test.go \
	$(SQLPARSER_ROOT)/help_messages.go \
	$(PKG_ROOT)/sql/lex/tokens.go \
	$(PKG_ROOT)/sql/lex/keywords.go \
	$(PKG_ROOT)/sql/lex/reserved_keywords.go

GO_PROTOS_TARGET := $(LOCAL_BIN)/.go_protobuf_sources
GW_PROTOS_TARGET := $(LOCAL_BIN)/.gw_protobuf_sources
CPP_PROTOS_TARGET := $(LOCAL_BIN)/.cpp_protobuf_sources
CPP_PROTOS_CCL_TARGET := $(LOCAL_BIN)/.cpp_ccl_protobuf_sources

.DEFAULT_GOAL := all
all: $(COCKROACH)

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss
buildoss: $(C_LIBS_OSS) $(UI_ROOT)/distoss/bindata.go

buildshort: BUILDTARGET = ./pkg/cmd/cockroach-short
buildshort: $(C_LIBS_CCL)

$(COCKROACH) build go-install gotestdashi generate lint lintshort: $(C_LIBS_CCL)
$(COCKROACH) build go-install generate: $(UI_ROOT)/distccl/bindata.go

$(COCKROACH) build buildoss buildshort: BUILDMODE = build -i -o $(COCKROACH)

BUILDINFO = .buildinfo/tag .buildinfo/rev

# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
$(COCKROACH) build buildoss buildshort go-install gotestdashi generate lint lintshort: \
	$(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET) $(SQLPARSER_TARGETS) $(BUILDINFO)
$(COCKROACH) build buildoss buildshort go-install gotestdashi generate lint lintshort: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)"

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: build buildoss buildshort install
build: ## Build the CockroachDB binary.
buildoss: ## Build the CockroachDB binary without any CCL-licensed code.
$(COCKROACH) build buildoss buildshort go-install:
	 $(XGO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

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
testbuild: gotestdashi
	$(XGO) list -tags '$(TAGS)' -f \
	'$(XGO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/{{.Name}}.test' $(PKG) | \
	$(SHELL)

.PHONY: gotestdashi
gotestdashi:
	$(XGO) test -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i $(PKG)

testshort: override TESTFLAGS += -short

testrace: ## Run tests with the Go race detector enabled.
testrace: override GOFLAGS += -race
testrace: export GORACE := halt_on_error=1
testrace: TESTTIMEOUT := $(RACETIMEOUT)

# Directory scans in the builder image are excruciatingly slow when running
# Docker for Mac, so we filter out the 20k+ UI dependencies that are
# guaranteed to be irrelevant to save nearly 10s on every Make invocation.
FIND_RELEVANT := find $(PKG_ROOT) -name node_modules -prune -o

bin/logictest.test: PKG := ./pkg/sql/logictest
bin/logictest.test: main.go $(shell $(FIND_RELEVANT) ! -name 'zcgo_flags.go' -name '*.go')
	$(MAKE) gotestdashi GOFLAGS='$(GOFLAGS)' TAGS='$(TAGS)' LINKFLAGS='$(LINKFLAGS)' PKG='$(PKG)'
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o bin/logictest.test $(PKG)

bin/logictestccl.test: PKG := ./pkg/ccl/sqlccl/logictestccl
bin/logictestccl.test: main.go $(shell $(FIND_RELEVANT) ! -name 'zcgo_flags.go' -name '*.go')
	$(MAKE) gotestdashi GOFLAGS='$(GOFLAGS)' TAGS='$(TAGS)' LINKFLAGS='$(LINKFLAGS)' PKG='$(PKG)'
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o bin/logictestccl.test $(PKG)


bench: ## Run benchmarks.
bench: TESTS := -
bench: BENCHES := .
bench: TESTTIMEOUT := $(BENCHTIMEOUT)

.PHONY: check test testshort testrace testlogic testccllogic bench
test: ## Run tests.
check test testshort testrace bench: gotestdashi
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

# Run make testlogic to run all of the logic tests. Specify test files to run
# with make testlogic FILES="foo bar".
testlogic: ## Run SQL Logic Tests.
testlogic: TESTS := $(if $(FILES),TestLogic$$//^$(subst $(space),$$|^,$(FILES))$$$(if $(SUBTESTS),/$(SUBTESTS)),TestLogic)
testlogic: TESTFLAGS := -test.v $(if $(FILES),-show-sql)
testlogic: bin/logictest.test
	cd pkg/sql/logictest && logictest.test -test.run "$(TESTS)" -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

# Run make testccllogic to run all of the CCL logic tests. Specify test files to run
# with make testccllogic FILES="foo bar".
testccllogic: ## Run SQL Logic Tests.
testccllogic: TESTS := $(if $(FILES),TestLogic$$//^$(subst $(space),$$|^,$(FILES))$$$(if $(SUBTESTS),/$(SUBTESTS)),TestLogic)
testccllogic: TESTFLAGS := -test.v $(if $(FILES),-show-sql)
testccllogic: bin/logictestccl.test
	cd pkg/ccl/sqlccl/logictestccl && logictestccl.test -test.run "$(TESTS)" -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

testraceslow: override GOFLAGS += -race
testraceslow: TESTTIMEOUT := $(RACETIMEOUT)

.PHONY: testslow testraceslow
testslow testraceslow: override TESTFLAGS += -v
testslow testraceslow: gotestdashi
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10

stressrace: override GOFLAGS += -race
stressrace: TESTTIMEOUT := $(RACETIMEOUT)

# Beware! This target is complicated because it needs to handle complexity:
# - PKG may be specified as relative (e.g. './gossip') or absolute (e.g.
# github.com/cockroachdb/cockroach/gossip), and this target needs to create
# the test binary in the correct location and `cd` to the correct directory.
# This is handled by having `go list` produce the command line.
# - PKG may also be recursive (e.g. './pkg/...'). This is also handled by piping
# through `go list`.
# - PKG may not contain any tests! This is handled with an `if` statement that
# checks for the presence of a test binary before running `stress` on it.
.PHONY: stress stressrace
stress: ## Run tests under stress.
stressrace: ## Run tests under stress with the race detector enabled.
stress stressrace: gotestdashi
	$(GO) list -tags '$(TAGS)' -f '$(XGO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o '\''{{.Dir}}'\''/stress.test && (cd '\''{{.Dir}}'\'' && if [ -f stress.test ]; then stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' $(if $(BENCHES),-test.bench '\''$(BENCHES)'\'') -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)

.PHONY: upload-coverage
upload-coverage: $(BOOTSTRAP_TARGET)
	$(GO) install ./vendor/github.com/wadey/gocovmerge
	$(GO) install ./vendor/github.com/mattn/goveralls
	@build/upload-coverage.sh

.PHONY: acceptance
acceptance: TESTTIMEOUT := $(ACCEPTANCETIMEOUT)
acceptance: export TESTTIMEOUT := $(TESTTIMEOUT)
acceptance: ## Run acceptance tests.
	@pkg/acceptance/run.sh

.PHONY: dupl
dupl: $(BOOTSTRAP_TARGET)
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
	$(GO) generate $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(PKG)

.PHONY: lint
lint: override TAGS += lint
lint: ## Run all style checkers and linters.
	@if [ -t 1 ]; then echo '$(yellow)NOTE: `make lint` is very slow! Perhaps `make lintshort`?$(term-reset)'; fi
	$(XGO) test $(PKG_ROOT)/testutils/lint -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run 'TestLint/$(TESTS)'

.PHONY: lintshort
lintshort: override TAGS += lint
lintshort: ## Run a fast subset of the style checkers and linters.
	$(XGO) test $(PKG_ROOT)/testutils/lint -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -run 'TestLint/$(TESTS)'

.PHONY: protobuf
protobuf: $(GO_PROTOS_TARGET) $(GW_PROTOS_TARGET) $(CPP_PROTOS_TARGET) $(CPP_PROTOS_CCL_TARGET)
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
	pkg/ui/distccl/bindata.go pkg/ui/distoss/bindata.go

# TODO(benesch): Make this recipe use `git ls-files --recurse-submodules`
# instead of scripts/ls-files.sh once Git v2.11 is widely deployed.
.INTERMEDIATE: $(ARCHIVE).tmp
$(ARCHIVE).tmp: ARCHIVE_BASE = cockroach-$(shell cat .buildinfo/tag)
$(ARCHIVE).tmp: $(ARCHIVE_EXTRAS)
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
	@{ git describe --tags --dirty 2> /dev/null || git rev-parse --short HEAD; } | tr -d \\n > $@

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

PROTOC_PLUGIN   := $(LOCAL_BIN)/protoc-gen-gogoroach
GOGOPROTO_PROTO := $(GOGO_PROTOBUF_PATH)/gogoproto/gogo.proto

COREOS_PATH := ./vendor/github.com/coreos
COREOS_RAFT_PROTOS := $(sort $(shell find $(COREOS_PATH)/etcd/raft -type f -name '*.proto'))

GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := ./vendor/$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes to the Go package containing the generated Go code.
PROTO_MAPPINGS :=
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,

GW_SERVER_PROTOS := $(PKG_ROOT)/server/serverpb/admin.proto $(PKG_ROOT)/server/serverpb/status.proto $(PKG_ROOT)/server/serverpb/authentication.proto
GW_TS_PROTOS := $(PKG_ROOT)/ts/tspb/timeseries.proto

GW_PROTOS  := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)
GW_SOURCES := $(GW_PROTOS:%.proto=%.pb.gw.go)

GO_PROTOS := $(sort $(shell $(FIND_RELEVANT) -type f -name '*.proto' -print))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

PBJS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbjs
PBTS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbts

UI_JS := $(UI_ROOT)/src/js/protos.js
UI_TS := $(UI_ROOT)/src/js/protos.d.ts
UI_PROTOS := $(UI_JS) $(UI_TS)

CPP_PROTOS := $(filter %/roachpb/metadata.proto %/roachpb/data.proto %/roachpb/internal.proto %/engine/enginepb/mvcc.proto %/engine/enginepb/mvcc3.proto %/engine/enginepb/registry.proto %/engine/enginepb/rocksdb.proto %/hlc/legacy_timestamp.proto %/hlc/timestamp.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst $(PKG_ROOT),$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.h))
CPP_SOURCES := $(subst $(PKG_ROOT),$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.cc))

CPP_PROTOS_CCL := $(filter %ccl/baseccl/encryption_options.proto,$(GO_PROTOS))
CPP_HEADERS_CCL := $(subst $(PKG_ROOT),$(CPP_PROTO_CCL_ROOT),$(CPP_PROTOS_CCL:%.proto=%.pb.h))
CPP_SOURCES_CCL := $(subst $(PKG_ROOT),$(CPP_PROTO_CCL_ROOT),$(CPP_PROTOS_CCL:%.proto=%.pb.cc))

UI_PROTOS := $(UI_JS) $(UI_TS)

$(GO_PROTOS_TARGET): $(PROTOC) $(PROTOC_PLUGIN) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	$(FIND_RELEVANT) -type f -name '*.pb.go' -exec rm {} +
	set -e; for dir in $(sort $(dir $(GO_PROTOS))); do \
	  build/werror.sh $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --plugin=$(PROTOC_PLUGIN) --gogoroach_out=$(PROTO_MAPPINGS),plugins=grpc,import_prefix=github.com/cockroachdb/cockroach/pkg/:$(PKG_ROOT) $$dir/*.proto; \
	done
	$(SED_INPLACE) '/import _/d' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!import (fmt|math) "github.com/cockroachdb/cockroach/pkg/(fmt|math)"! !g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!cockroachdb/cockroach/pkg/(etcd)!coreos/\1!g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!github.com/cockroachdb/cockroach/pkg/(bytes|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)!\1!g' $(GO_SOURCES)
	gofmt -s -w $(GO_SOURCES)
	touch $@

$(GW_PROTOS_TARGET): $(PROTOC) $(GW_SERVER_PROTOS) $(GW_TS_PROTOS) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	$(FIND_RELEVANT) -type f -name '*.pb.gw.go' -exec rm {} +
	build/werror.sh $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:$(PKG_ROOT) $(GW_SERVER_PROTOS)
	build/werror.sh $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:$(PKG_ROOT) $(GW_TS_PROTOS)
	touch $@

$(CPP_PROTOS_TARGET): $(PROTOC) $(CPP_PROTOS)
	$(FIND_RELEVANT) -type f \( -name '*.pb.h' -o -name '*.pb.cc' \) -exec rm {} +
	mkdir -p $(CPP_PROTO_ROOT)
	build/werror.sh $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(CPP_PROTO_ROOT) $(CPP_PROTOS)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS) $(CPP_SOURCES)
	touch $@

$(CPP_PROTOS_CCL_TARGET): $(PROTOC) $(CPP_PROTOS_CCL)
	$(FIND_RELEVANT) -type f \( -name '*.pb.h' -o -name '*.pb.cc' \) -exec rm {} +
	mkdir -p $(CPP_PROTO_CCL_ROOT)
	build/werror.sh $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(CPP_PROTO_CCL_ROOT) $(CPP_PROTOS_CCL)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS_CCL) $(CPP_SOURCES_CCL)
	touch $@

.SECONDARY: $(UI_JS)
$(UI_JS): $(GO_PROTOS) $(COREOS_RAFT_PROTOS) $(YARN_INSTALLED_TARGET)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBJS) -t static-module -w es6 --strict-long --keep-case --path $(PKG_ROOT) --path $(GOGO_PROTOBUF_PATH) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(GW_PROTOS) >> $@

.SECONDARY: $(UI_TS)
$(UI_TS): $(UI_JS) $(YARN_INSTALLED_TARGET)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBTS) $(UI_JS) >> $@

STYLINT            := ./node_modules/.bin/stylint
TSLINT             := ./node_modules/.bin/tslint
KARMA              := ./node_modules/.bin/karma
WEBPACK            := ./node_modules/.bin/webpack
WEBPACK_DEV_SERVER := ./node_modules/.bin/webpack-dev-server
WEBPACK_DASHBOARD  := ./opt/node_modules/.bin/webpack-dashboard

.PHONY: ui-generate
ui-generate: $(UI_ROOT)/distccl/bindata.go

.PHONY: ui-lint
ui-lint: $(UI_PROTOS)
	$(NODE_RUN) -C $(UI_ROOT) $(STYLINT) -c .stylintrc styl
	$(NODE_RUN) -C $(UI_ROOT) $(TSLINT) -c tslint.json -p tsconfig.json --type-check
	@# TODO(benesch): Invoke tslint just once when palantir/tslint#2827 is fixed.
	$(NODE_RUN) -C $(UI_ROOT) $(TSLINT) -c tslint.json *.js
	@if $(NODE_RUN) -C $(UI_ROOT) yarn list | grep phantomjs; then echo ^ forbidden UI dependency >&2; exit 1; fi

# DLLs are Webpack bundles, not Windows shared libraries. See "DLLs for speedy
# builds" in the UI README for details.
UI_DLLS := $(UI_ROOT)/dist/protos.dll.js $(UI_ROOT)/dist/vendor.dll.js
UI_MANIFESTS := $(UI_ROOT)/protos-manifest.json $(UI_ROOT)/vendor-manifest.json

# (Ab)use a pattern rule to teach Make that this one command produces two files.
# Normally, it would run the recipe twice if dist/FOO.js and FOO-manifest.js
# were both out-of-date. [0]
#
# XXX: Ideally we'd scope the dependency on $(UI_PROTOS) to the protos DLL, but
# Make v3.81 has a bug that causes the dependency to be ignored [1]. We're stuck
# with this workaround until Apple decides to update the version of Make they
# ship with macOS or we require a newer version of Make. Such a requirement
# would need to be strictly enforced, as the way this fails is extremely subtle
# and doesn't present until the web UI is loaded in the browser.
#
# [0]: https://stackoverflow.com/a/3077254/1122351
# [1]: http://savannah.gnu.org/bugs/?19108
.SECONDARY: $(UI_DLLS) $(UI_MANIFESTS)
$(UI_ROOT)/dist/%.dll.js $(UI_ROOT)/%-manifest.json: $(UI_ROOT)/webpack.%.js $(YARN_INSTALLED_TARGET) $(UI_PROTOS)
	$(NODE_RUN) -C $(UI_ROOT) $(WEBPACK) -p --config webpack.$*.js

.PHONY: ui-test
ui-test: $(UI_DLLS) $(UI_MANIFESTS)
	$(NODE_RUN) -C $(UI_ROOT) $(KARMA) start

.PHONY: ui-test-watch
ui-test-watch: $(UI_DLLS) $(UI_MANIFESTS)
	$(NODE_RUN) -C $(UI_ROOT) $(KARMA) start --no-single-run --auto-watch

$(UI_ROOT)/dist%/bindata.go: $(UI_ROOT)/webpack.%.js $(UI_DLLS) $(UI_JS) $(UI_MANIFESTS) $(shell find $(UI_ROOT)/src $(UI_ROOT)/styl -type f)
	@# TODO(benesch): remove references to embedded.go once sufficient time has passed.
	rm -f $(UI_ROOT)/embedded.go
	find $(UI_ROOT)/dist$* -mindepth 1 -not -name dist$*.go -delete
	set -e; for dll in $(notdir $(UI_DLLS)); do ln -s ../dist/$$dll $(UI_ROOT)/dist$*/$$dll; done
	$(NODE_RUN) -C $(UI_ROOT) $(WEBPACK) --config webpack.$*.js
	go-bindata -pkg dist$* -o $@ -prefix $(UI_ROOT)/dist$* $(UI_ROOT)/dist$*/...
	$(SED_INPLACE) -f $(UI_ROOT)/process-bindata.sed $@
	gofmt -s -w $@

$(UI_ROOT)/yarn.opt.installed:
	$(NODE_RUN) -C $(UI_ROOT)/opt yarn install
	touch $@

.PHONY: ui-watch
ui-watch: export TARGET ?= http://localhost:8080
ui-watch: PORT := 3000
ui-watch: $(UI_DLLS) $(UI_ROOT)/yarn.opt.installed
	cd $(UI_ROOT) && $(WEBPACK_DASHBOARD) -- $(WEBPACK_DEV_SERVER) --config webpack.ccl.js --port $(PORT)

.PHONY: ui-maintainer-clean
ui-maintainer-clean: ## Like clean, but also remove some auto-generated source code.
	find $(UI_ROOT)/dist* -mindepth 1 -not -name dist*.go -delete
	rm -rf $(UI_ROOT)/node_modules $(UI_DLLS) $(YARN_INSTALLED_TARGET)

.SECONDARY: $(SQLPARSER_ROOT)/gen/sql.go.tmp
$(SQLPARSER_ROOT)/gen/sql.go.tmp: $(SQLPARSER_ROOT)/gen/sql.y $(BOOTSTRAP_TARGET)
	set -euo pipefail; \
	  ret=$$(cd $(SQLPARSER_ROOT)/gen && goyacc -p sql -o sql.go.tmp sql.y); \
	  if expr "$$ret" : ".*conflicts" >/dev/null; then \
	    echo "$$ret"; exit 1; \
	  fi

# The lex package needs to know about all tokens, because the encode
# functions and lexing predicates need to know about keywords, and
# keywords map to the token constants. Therefore, generate the
# constant tokens in the lex package primarily.
$(PKG_ROOT)/sql/lex/tokens.go: $(SQLPARSER_ROOT)/gen/sql.go.tmp
	(echo "// Code generated by make. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 echo; \
	 echo "package lex"; \
	 echo; \
	 grep '^const [A-Z][_A-Z0-9]* ' $^) > $@


# The lex package is now the primary source for the token constant
# definitions. Modify the code generated by goyacc here to refer to
# the definitions in the lex package.
$(SQLPARSER_ROOT)/sql.go: $(SQLPARSER_ROOT)/gen/sql.go.tmp
	(echo "// Code generated by goyacc. DO NOT EDIT."; \
	 echo "// GENERATED FILE DO NOT EDIT"; \
	 cat $^ | \
	 sed -E 's/^const ([A-Z][_A-Z0-9]*) =.*$$/const \1 = lex.\1/g') > $@

# This modifies the grammar to:
# - improve the types used by the generated parser for non-terminals
# - expand the help rules.
#
# For types:
# Determine the types that will be migrated to union types by looking
# at the accessors of sqlSymUnion. The first step in this pipeline
# prints every return type of a sqlSymUnion accessor on a separate line.
# The next step regular expression escapes these types. The third step
# joins all of the lines into a single line with a '|' character to be
# used as a regexp "or" meta-character. Finally, the last '|' character
# is stripped from the string.
# Then translate the original syntax file, with the types determined
# above being replaced with the union type in their type declarations.
.SECONDARY: $(SQLPARSER_ROOT)/gen/sql.y
$(SQLPARSER_ROOT)/gen/sql.y: $(SQLPARSER_ROOT)/sql.y $(SQLPARSER_ROOT)/replace_help_rules.awk
	mkdir -p $(SQLPARSER_ROOT)/gen
	set -euo pipefail; \
	TYPES=$$(awk '/func.*sqlSymUnion/ {print $$(NF - 1)}' $(SQLPARSER_ROOT)/sql.y | \
	        sed -e 's/[]\/$$*.^|[]/\\&/g' | \
	        tr '\n' '|' | \
	        sed -E '$$s/.$$//'); \
	sed -E "s_(type|token) <($$TYPES)>_\1 <union> /* <\2> */_" < $(SQLPARSER_ROOT)/sql.y | \
	awk -f $(SQLPARSER_ROOT)/replace_help_rules.awk > $@

$(PKG_ROOT)/sql/lex/reserved_keywords.go: $(SQLPARSER_ROOT)/sql.y $(SQLPARSER_ROOT)/reserved_keywords.awk
	awk -f $(SQLPARSER_ROOT)/reserved_keywords.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

$(PKG_ROOT)/sql/lex/keywords.go: $(SQLPARSER_ROOT)/sql.y $(SQLPARSER_ROOT)/all_keywords.awk
	awk -f $(SQLPARSER_ROOT)/all_keywords.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

# This target will print unreserved_keywords which are not actually
# used in the grammar.
.PHONY: unused_unreserved_keywords
unused_unreserved_keywords: $(SQLPARSER_ROOT)/sql.y $(SQLPARSER_ROOT)/unreserved_keywords.awk
	@for kw in $$(awk -f unreserved_keywords.awk < $<); do \
	  if [ $$(grep -c $${kw} $<) -le 2 ]; then \
	    echo $${kw}; \
	  fi \
	done

$(SQLPARSER_ROOT)/helpmap_test.go: $(SQLPARSER_ROOT)/gen/sql.y $(SQLPARSER_ROOT)/help_gen_test.sh
	@$(SQLPARSER_ROOT)/help_gen_test.sh < $< >$@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

$(SQLPARSER_ROOT)/help_messages.go: $(SQLPARSER_ROOT)/sql.y $(SQLPARSER_ROOT)/help.awk
	awk -f $(SQLPARSER_ROOT)/help.awk < $< > $@.tmp || rm $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@

# Format libroach .cc and .h files (excluding protos) using clang-format if installed.
# We also exclude the auto-generated keys.h
.PHONY: c-deps-fmt
c-deps-fmt: $(shell find $(LIBROACH_SRC_DIR) \( -name '*.cc' -o -name '*.h' \) -not \( -name '*.pb.cc' -o -name '*.pb.h' -o -name 'keys.h' \))
	clang-format -i $^

.PHONY: clean-c-deps
clean-c-deps:
	rm -rf $(CRYPTOPP_DIR)
	rm -rf $(JEMALLOC_DIR)
	rm -rf $(PROTOBUF_DIR)
	rm -rf $(ROCKSDB_DIR)
	rm -rf $(SNAPPY_DIR)

.PHONY: unsafe-clean-c-deps
unsafe-clean-c-deps:
	git -C $(CRYPTOPP_SRC_DIR) clean -dxf
	git -C $(JEMALLOC_SRC_DIR) clean -dxf
	git -C $(PROTOBUF_SRC_DIR) clean -dxf
	git -C $(ROCKSDB_SRC_DIR)  clean -dxf
	git -C $(SNAPPY_SRC_DIR)   clean -dxf

.PHONY: clean
clean: ## Remove build artifacts.
clean: clean-c-deps
	rm -rf $(GO_PROTOS_TARGET) $(GW_PROTOS_TARGET) $(CPP_PROTOS_TARGET)
	$(GO) clean $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i github.com/cockroachdb/...
	$(FIND_RELEVANT) -type f \( -name 'zcgo_flags*.go' -o -name '*.test' \) -exec rm {} +
	for f in cockroach*; do if [ -f "$$f" ]; then rm "$$f"; fi; done
	rm -rf artifacts $(LOCAL_BIN) $(ARCHIVE) $(SQLPARSER_ROOT)/gen

.PHONY: maintainer-clean
maintainer-clean: ## Like clean, but also remove some auto-generated source code.
maintainer-clean: clean ui-maintainer-clean
	rm -f $(SQLPARSER_TARGETS) $(UI_PROTOS)

.PHONY: unsafe-clean
unsafe-clean: ## Like maintainer-clean, but also remove ALL untracked/ignored files.
unsafe-clean: maintainer-clean unsafe-clean-c-deps
	git clean -dxf

.SECONDEXPANSION:
$(LOCAL_BIN)/%: $$(shell find $(PKG_ROOT)/cmd/$$*) | $(SUBMODULES_TARGET)
	@$(GO_INSTALL) -v $(PKG_ROOT)/cmd/$*
