# Copyright 2017 The Cockroach Authors.
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
#
# Author: Nikhil Benesch (nikhil.benesch@gmail.com)

# This file defines variables and targets that are used by all Makefiles in the
# project. The including Makefile must define REPO_ROOT to the relative path to
# the root of the repository before including this file.

# Variables to control executable names. These can be overridden in the
# environment or on the command line, e.g.
#   GOFLAGS=-msan make build     OR     make build GOFLAGS=-msan
GO      ?= go
GOFLAGS ?=
XGO     ?= xgo
TAR     ?= tar

# Convenience variables for important paths.
ORG_ROOT := $(REPO_ROOT)/..
PKG_ROOT := $(REPO_ROOT)/pkg
UI_ROOT  := $(PKG_ROOT)/ui

# Ensure we have an unambiguous GOPATH.
export GOPATH := $(realpath $(ORG_ROOT)/../../..)
#                                       ^  ^  ^~ GOPATH
#                                       |  |~ GOPATH/src
#                                       |~ GOPATH/src/github.com

# Avoid printing twice if Make restarts (because a Makefile was changed) or is
# called recursively from another Makefile.
ifeq ($(MAKE_RESTARTS)$(MAKELEVEL),0)
$(info GOPATH set to $(GOPATH))
endif

# We install our vendored tools to a directory within this repository to avoid
# overwriting any user-installed binaries of the same name in the default GOBIN.
LOCAL_BIN := $(abspath $(REPO_ROOT)/bin)
GO_INSTALL := GOBIN='$(LOCAL_BIN)' $(GO) install

# Prefer tools we've installed with go install and Yarn to those elsewhere on
# the PATH.
#
# Usually, we could use the yarn run command to avoid changing the PATH
# globally. Unfortunately, yarn run must be executed in or beneath UI_ROOT, but
# protobuf.mk, which depends on Yarn-installed executables, needs to execute in
# ORG_ROOT. It's much simpler to add the Yarn executable-installation directory
# to the PATH than have protobuf.mk adjust its paths to work in both ORG_ROOT
# and UI_ROOT.
export PATH := $(LOCAL_BIN):$(UI_ROOT)/node_modules/.bin:$(PATH)

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

# make-lazy converts a recursive variable, which is evaluated every time it's
# referenced, to a lazy variable, which is evaluated only the first time it's
# used. See: http://blog.jgc.org/2016/07/lazy-gnu-make-variables.html
override make-lazy = $(eval $1 = $$(eval $1 := $(value $1))$$($1))

UNAME := $(shell uname)
MACOS := $(findstring Darwin,$(UNAME))
MINGW := $(findstring MINGW,$(UNAME))

NCPUS = $(shell $(LOCAL_BIN)/ncpus)
$(call make-lazy,NCPUS)

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

# We used to check the Go version in a .PHONY .go-version target, but the error
# message, if any, would get mixed in with noise from other targets if Make was
# executed in parallel job mode. This check, by contrast, is guaranteed to print
# its error message before any noisy output.
#
# Note that word boundary markers (\b, \<, [[:<:]]) are not portable, but `grep
# -w`, though not required by POSIX, exists on all tested platforms.
include $(REPO_ROOT)/.go-version
ifeq ($(shell $(GO) version | grep -qwE '$(GOVERS)' && echo y),)
$(error "$(GOVERS) required (see CONTRIBUTING.md): $(shell $(GO) version)")
endif

# Print an error if the user specified any variables on the command line that
# don't appear in this Makefile. The list of valid variables is automatically
# rebuilt on the first successful `make` invocation after the Makefile changes.
include $(REPO_ROOT)/build/variables.mk
$(foreach v,$(filter-out $(strip $(VALID_VARS)),$(.VARIABLES)),\
	$(if $(findstring command line,$(origin $v)),$(error Variable `$v' is not recognized by this Makefile)))
-include $(REPO_ROOT)/customenv.mk

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
GITHOOKSDIR := $(shell test -d $(REPO_ROOT)/.git && echo '$(REPO_ROOT)/.git/hooks' || git rev-parse --git-path hooks)
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

$(YARN_INSTALLED_TARGET): $(BOOTSTRAP_TARGET) $(UI_ROOT)/package.json $(UI_ROOT)/yarn.lock
	cd $(UI_ROOT) && yarn install
	rm -rf $(UI_ROOT)/node_modules/@types/node # https://github.com/yarnpkg/yarn/issues/2987
	touch $@

# We store the bootstrap marker file in the bin directory so that remapping bin,
# like we do in the builder container to allow for different host and guest
# systems, will trigger bootstrapping in the container as necessary. This is
# extracted into a variable for the same reasons as YARN_INSTALLED_TARGET.
BOOTSTRAP_TARGET := $(LOCAL_BIN)/.bootstrap

# Update the git hooks and install commands from dependencies whenever they
# change.
$(BOOTSTRAP_TARGET): $(GITHOOKS) $(REPO_ROOT)/glide.lock
ifneq ($(GIT_DIR),)
	git submodule update --init
endif
	@$(GO_INSTALL) -v $(PKG_ROOT)/cmd/{metacheck,ncpus,returncheck} \
	$$($(GO) list -tags glide -f '{{join .Imports "\n"}}' $(REPO_ROOT)/build)
	touch $@

# Make doesn't expose a list of the variables declared in a given file, so we
# resort to sed magic. Roughly, this sed command prints VARIABLE in lines of the
# following forms:
#
#     [export] VARIABLE [:+?]=
#     TARGET-NAME: [export] VARIABLE [:+?]=
#
# The additional complexity below handles whitespace and comments.
$(REPO_ROOT)/build/variables.mk: $(REPO_ROOT)/Makefile $(REPO_ROOT)/.go-version $(REPO_ROOT)/build/common.mk $(REPO_ROOT)/build/archive/contents/Makefile
	@echo '# This file is auto-generated by Make.' > $@
	@echo '# DO NOT EDIT!' >> $@
	@echo 'define VALID_VARS' >> $@
	@sed -nE -e '/^	/d' -e 's/([^#]*)#.*/\1/' \
	  -e 's/(^|^[^:]+:)[ ]*(export)?[ ]*([^ ]+)[ ]*[:?+]?=.*/  \3/p' $^ \
	  | LC_COLLATE=C sort -u >> $@
	@echo 'endef' >> $@

# The following section handles building our C/C++ dependencies. These are
# common because both the root Makefile and protobuf.mk have C dependencies.

C_DEPS_DIR := $(abspath $(REPO_ROOT)/c-deps)
JEMALLOC_SRC_DIR := $(C_DEPS_DIR)/jemalloc.src
PROTOBUF_SRC_DIR := $(C_DEPS_DIR)/protobuf.src
ROCKSDB_SRC_DIR  := $(C_DEPS_DIR)/rocksdb.src
SNAPPY_SRC_DIR   := $(C_DEPS_DIR)/snappy.src

C_LIBS_SRCS := $(JEMALLOC_SRC_DIR) $(PROTOBUF_SRC_DIR) $(ROCKSDB_SRC_DIR) $(SNAPPY_SRC_DIR)

HOST_TRIPLE := $(shell $$($(GO) env CC) -dumpmachine)

CONFIGURE_FLAGS :=
CMAKE_FLAGS := $(if $(MINGW),-G 'MSYS Makefiles')

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

CONFIGURE_FLAGS += --host=$(XHOST_TRIPLE)
CMAKE_FLAGS += -DCMAKE_C_COMPILER=$(CC_PATH) -DCMAKE_CXX_COMPILER=$(CXX_PATH) -DCMAKE_SYSTEM_NAME=$(CMAKE_SYSTEM_NAME)

TARGET_TRIPLE := $(XHOST_TRIPLE)
else
TARGET_TRIPLE := $(HOST_TRIPLE)
endif

BUILD_DIR := $(GOPATH)/native/$(TARGET_TRIPLE)

# In MinGW, cgo flags don't handle Unix-style paths, so convert our base path to
# a Windows-style path.
#
# TODO(benesch): Figure out why. MinGW transparently converts Unix-style paths
# everywhere else.
ifdef MINGW
BUILD_DIR := $(shell cygpath -m $(BUILD_DIR))
endif

JEMALLOC_DIR := $(BUILD_DIR)/jemalloc
PROTOBUF_DIR := $(BUILD_DIR)/protobuf
ROCKSDB_DIR  := $(BUILD_DIR)/rocksdb$(if $(ENABLE_ROCKSDB_ASSERTIONS),_assert)
SNAPPY_DIR   := $(BUILD_DIR)/snappy
# Can't share with protobuf because protoc is always built for the host.
PROTOC_DIR := $(GOPATH)/native/$(HOST_TRIPLE)/protobuf
PROTOC 		 := $(PROTOC_DIR)/protoc

C_LIBS := libjemalloc libprotobuf libsnappy librocksdb

# Go does not permit dashes in build tags. This is undocumented. Fun!
TARGET_TRIPLE_TAG := $(subst -,_,$(TARGET_TRIPLE))

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
CGO_SUFFIXED_FLAGS_FILES   := $(addprefix $(PKG_ROOT)/,$(addsuffix /zcgo_flags_$(TARGET_TRIPLE_TAG).go,$(CGO_PKGS)))
CGO_FLAGS_FILES := $(CGO_UNSUFFIXED_FLAGS_FILES) $(CGO_SUFFIXED_FLAGS_FILES)

$(CGO_UNSUFFIXED_FLAGS_FILES): .ALWAYS_REBUILD

$(CGO_FLAGS_FILES): $(REPO_ROOT)/build/common.mk
	@echo 'GEN $@'
	@echo '// GENERATED FILE DO NOT EDIT' > $@
	@echo >> $@
	@echo '// +build $(if $(findstring $(TARGET_TRIPLE_TAG),$@),$(TARGET_TRIPLE_TAG),!make)' >> $@
	@echo >> $@
	@echo 'package $(notdir $(@D))' >> $@
	@echo >> $@
	@echo '// #cgo CPPFLAGS: -I$(JEMALLOC_DIR)/include' >> $@
	@echo '// #cgo LDFLAGS: $(addprefix -L,$(PROTOBUF_DIR) $(JEMALLOC_DIR)/lib $(SNAPPY_DIR)/.libs $(ROCKSDB_DIR))' >> $@
	@echo 'import "C"' >> $@

# We package tarballs in c-deps so that DEP.src.tar.xz is guaranteed to extract
# to folder DEP.src.
#
# Targeting a marker file, DEP.src/.extracted, instead of directly targeting the
# DEP.src folder, guarantees that the tarball extraction is atomic, as the
# marker file is touched only after a successful extraction. Otherwise, it's
# quite common for a mistimed Ctrl-C to leave DEP.src partially-extracted but
# with a timestamp that makes it appear up-to-date.
#
# This is the behavior provided by .DELETE_ON_ERROR, but .DELETE_ON_ERROR only
# works on file targets, not folder targets.
$(C_DEPS_DIR)/%.src/.extracted: $(C_DEPS_DIR)/%.src.tar.xz
	rm -rf $(notdir $(@D))
	$(REPO_ROOT)/scripts/untarxz.sh $< -C $(C_DEPS_DIR)
	touch $@

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
# NB: the recipes below nuke *all* build artifacts when a dependency's source
# tarball changes. Normally, we could rely on the dependency's build system to
# detect modified files and only rebuild the necessary objects, but tar assigns
# extracted files their original modification time (i.e., a time well in the
# past, when the tarballs were packaged), and so the build artifacts always look
# up-to-date.

$(JEMALLOC_DIR)/Makefile: $(C_DEPS_DIR)/jemalloc-rebuild $(JEMALLOC_SRC_DIR)/.extracted
	rm -rf $(JEMALLOC_DIR)
	mkdir -p $(JEMALLOC_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/jemalloc-rebuild. See above for rationale.
	@#
	@# jemalloc profiling deadlocks when built against musl. See
	@# https://github.com/jemalloc/jemalloc/issues/585.
	cd $(JEMALLOC_DIR) && $(JEMALLOC_SRC_DIR)/configure $(CONFIGURE_FLAGS) $(if $(findstring musl,$(TARGET_TRIPLE)),,--enable-prof)

$(PROTOBUF_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild $(PROTOBUF_SRC_DIR)/.extracted
	rm -rf $(PROTOBUF_DIR)
	mkdir -p $(PROTOBUF_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOBUF_DIR) && cmake $(CMAKE_FLAGS) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake

ifneq ($(PROTOC_DIR),$(PROTOBUF_DIR))
$(PROTOC_DIR)/Makefile: $(C_DEPS_DIR)/protobuf-rebuild $(PROTOBUF_SRC_DIR)/.extracted
	rm -rf $(PROTOC_DIR)
	mkdir -p $(PROTOC_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/protobuf-rebuild. See above for rationale.
	cd $(PROTOC_DIR) && cmake $(CMAKE_FLAGS) -Dprotobuf_BUILD_TESTS=OFF $(PROTOBUF_SRC_DIR)/cmake
endif

$(ROCKSDB_DIR)/Makefile: $(C_DEPS_DIR)/rocksdb-rebuild $(ROCKSDB_SRC_DIR)/.extracted | libsnappy libjemalloc
	rm -rf $(ROCKSDB_DIR)
	mkdir -p $(ROCKSDB_DIR)
	@# NOTE: If you change the CMake flags below, bump the version in
	@# $(C_DEPS_DIR)/rocksdb-rebuild. See above for rationale.
	cd $(ROCKSDB_DIR) && cmake $(CMAKE_FLAGS) $(ROCKSDB_SRC_DIR) \
	  $(if $(findstring release,$(TYPE)),,-DWITH_$(if $(findstring mingw,$(TARGET_TRIPLE)),AVX2,SSE42)=OFF) \
	  -DSNAPPY_LIBRARIES=$(SNAPPY_DIR)/.libs/libsnappy.a -DSNAPPY_INCLUDE_DIR=$(SNAPPY_SRC_DIR) -DWITH_SNAPPY=ON \
	  -DJEMALLOC_LIBRARIES=$(JEMALLOC_DIR)/lib/libjemalloc.a -DJEMALLOC_INCLUDE_DIR=$(JEMALLOC_DIR)/include -DWITH_JEMALLOC=ON \
	  $(if $(ENABLE_ROCKSDB_ASSERTIONS),,-DCMAKE_CXX_FLAGS=-DNDEBUG)
	@# TODO(benesch): Tweak how we pass -DNDEBUG above when we upgrade to a
	@# RocksDB release that includes https://github.com/facebook/rocksdb/pull/2300.

$(SNAPPY_DIR)/Makefile: $(C_DEPS_DIR)/snappy-rebuild $(SNAPPY_SRC_DIR)/.extracted
	rm -rf $(SNAPPY_DIR)
	mkdir -p $(SNAPPY_DIR)
	@# NOTE: If you change the configure flags below, bump the version in
	@# $(C_DEPS_DIR)/snappy-rebuild. See above for rationale.
	cd $(SNAPPY_DIR) && $(SNAPPY_SRC_DIR)/configure $(CONFIGURE_FLAGS) --disable-shared

# We mark C and C++ dependencies as .PHONY (or .ALWAYS_REBUILD) to avoid
# having to name the artifact (for .PHONY), which can vary by platform, and so
# the child Makefile can determine whether the target is up to date (for both
# .PHONY and .ALWAYS_REBUILD). We don't have the targets' prerequisites here,
# and we certainly don't want to duplicate them.

$(PROTOC): $(PROTOC_DIR)/Makefile $(BOOTSTRAP_TARGET) .ALWAYS_REBUILD
	@$(MAKE) --no-print-directory -C $(PROTOC_DIR) -j$(NCPUS) protoc

.PHONY: libjemalloc
libjemalloc: $(JEMALLOC_DIR)/Makefile $(BOOTSTRAP_TARGET)
	@$(MAKE) --no-print-directory -C $(JEMALLOC_DIR) -j$(NCPUS) build_lib_static

.PHONY: libprotobuf
libprotobuf: $(PROTOBUF_DIR)/Makefile $(BOOTSTRAP_TARGET)
	@$(MAKE) --no-print-directory -C $(PROTOBUF_DIR) -j$(NCPUS) libprotobuf

.PHONY: libsnappy
libsnappy: $(SNAPPY_DIR)/Makefile $(BOOTSTRAP_TARGET)
	@$(MAKE) --no-print-directory -C $(SNAPPY_DIR) -j$(NCPUS)

.PHONY: librocksdb
librocksdb: $(ROCKSDB_DIR)/Makefile $(BOOTSTRAP_TARGET)
	@$(MAKE) --no-print-directory -C $(ROCKSDB_DIR) -j$(NCPUS) rocksdb

.PHONY: clean-c-deps
clean-c-deps:
	rm -rf $(JEMALLOC_DIR)
	rm -rf $(PROTOBUF_DIR)
	rm -rf $(ROCKSDB_DIR)
	rm -rf $(SNAPPY_DIR)
