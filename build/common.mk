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

OS := $(shell uname)
OS_WINDOWS := $(findstring MINGW,$(OS))

CMAKE_FLAGS = $(if $(OS_WINDOWS),-G 'MSYS Makefiles')

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
include $(REPO_ROOT)/.go-version
ifeq ($(shell $(GO) version | grep -q -E '\b$(GOVERS)\b' && echo y),)
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
	$(GO_INSTALL) -v \
	$(PKG_ROOT)/cmd/metacheck \
	$(PKG_ROOT)/cmd/returncheck \
	$(PKG_ROOT)/cmd/ncpus \
	&& $(GO) list -tags glide -f '{{join .Imports "\n"}}' $(REPO_ROOT)/build | xargs $(GO) install -v
	touch $@

# Make doesn't expose a list of the variables declared in a given file, so we
# resort to sed magic. Roughly, this sed command prints VARIABLE in lines of the
# following forms:
#
#     [export] VARIABLE [:+?]=
#     TARGET-NAME: [export] VARIABLE [:+?]=
#
# The additional complexity below handles whitespace and comments.
$(REPO_ROOT)/build/variables.mk: $(REPO_ROOT)/Makefile $(REPO_ROOT)/.go-version $(REPO_ROOT)/build/common.mk
	@echo '# This file is auto-generated by Make.' > $@
	@echo '# DO NOT EDIT!' >> $@
	@echo 'define VALID_VARS' >> $@
	@sed -nE -e '/^	/d' -e 's/([^#]*)#.*/\1/' \
	  -e 's/(^|^[^:]+:)[ ]*(export)?[ ]*([^ ]+)[ ]*[:?+]?=.*/  \3/p' $^ \
	  | LC_COLLATE=C sort -u >> $@
	@echo 'endef' >> $@

# The following section handles building our C/C++ dependencies. These are
# common because both the root Makefile and protobuf.mk have C dependencies.

C_DEPS_DIR := $(REPO_ROOT)/c-deps

PROTOBUF_DIR := $(C_DEPS_DIR)/protobuf
JEMALLOC_DIR := $(C_DEPS_DIR)/jemalloc
SNAPPY_DIR   := $(C_DEPS_DIR)/snappy
ROCKSDB_DIR  := $(C_DEPS_DIR)/rocksdb

PROTOC := $(PROTOBUF_DIR)/protoc

LIBPROTOBUF := $(PROTOBUF_DIR)/libprotobuf.a
LIBJEMALLOC := $(JEMALLOC_DIR)/lib/libjemalloc.a
LIBSNAPPY   := $(SNAPPY_DIR)/.libs/libsnappy.a
LIBROCKSDB  := $(ROCKSDB_DIR)/librocksdb.a

JEMALLOC_INCLUDES := $(JEMALLOC_DIR)/include
SNAPPY_INCLUDES   := $(SNAPPY_DIR)

C_LIBS      := $(LIBJEMALLOC) $(LIBPROTOBUF) $(LIBSNAPPY) $(LIBROCKSDB)
C_DEPS      := $(PROTOC) $(C_LIBS)
C_DEPS_DIRS := $(PROTOBUF_DIR) $(JEMALLOC_DIR) $(SNAPPY_DIR) $(ROCKSDB_DIR)

# We package tarballs in c-deps so that DEP.src.tgz is guaranteed to extract to
# folder DEP.
$(C_DEPS_DIR)/%: $(C_DEPS_DIR)/%.src.tgz $(BOOTSTRAP_TARGET)
	rm -rf $@
	tar -C $(C_DEPS_DIR) -zxf $<
	touch $@

$(PROTOBUF_DIR)/Makefile: $(C_DEPS_DIR)/protobuf.src.tgz | $(PROTOBUF_DIR)
	cd $(PROTOBUF_DIR) && cmake $(CMAKE_FLAGS) ./cmake

$(JEMALLOC_DIR)/Makefile: $(C_DEPS_DIR)/jemalloc.src.tgz | $(JEMALLOC_DIR)
	cd $(JEMALLOC_DIR) && ./configure

$(SNAPPY_DIR)/Makefile: $(C_DEPS_DIR)/snappy.src.tgz | $(SNAPPY_DIR)
	cd $(SNAPPY_DIR) && ./configure --disable-shared

$(ROCKSDB_DIR)/Makefile: $(C_DEPS_DIR)/rocksdb.src.tgz | $(ROCKSDB_DIR)
	cd $(ROCKSDB_DIR) && cmake $(CMAKE_FLAGS) \
	  -DSNAPPY_INCLUDE_DIR=../../$(SNAPPY_INCLUDES) -DSNAPPY_LIBRARIES=../../$(LIBSNAPPY) -DWITH_SNAPPY=ON \
	  -DJEMALLOC_INCLUDE_DIR=../../$(JEMALLOC_INCLUDES) -DJEMALLOC_LIBRARIES=../../$(LIBJEMALLOC) -DWITH_JEMALLOC=ON

# We mark C and C++ dependencies as .ALWAYS_REBUILD so the child Makefile can
# determine whether the target is up to date. We don't have the targets'
# prerequisites here, and we certainly don't want to duplicate them.

$(PROTOC): $(PROTOBUF_DIR)/Makefile .ALWAYS_REBUILD
	$(MAKE) -C $(PROTOBUF_DIR) -j$(NCPUS) protoc

$(LIBPROTOBUF): $(PROTOBUF_DIR)/Makefile .ALWAYS_REBUILD
	$(MAKE) -C $(PROTOBUF_DIR) -j$(NCPUS) libprotobuf

$(LIBJEMALLOC): $(JEMALLOC_DIR)/Makefile .ALWAYS_REBUILD
	$(MAKE) -C $(JEMALLOC_DIR) -j$(NCPUS) build_lib_static LIBPREFIX=lib A=a

$(LIBSNAPPY): $(SNAPPY_DIR)/Makefile .ALWAYS_REBUILD
	$(MAKE) -C $(SNAPPY_DIR) -j$(NCPUS) libsnappy.la

$(LIBROCKSDB): $(ROCKSDB_DIR)/Makefile $(LIBSNAPPY) $(LIBJEMALLOC) .ALWAYS_REBUILD
	$(MAKE) -C $(ROCKSDB_DIR) -j$(NCPUS) rocksdb

.PHONY: c-libs
c-libs: $(C_LIBS)

.PHONY: c-deps
c-deps: $(C_DEPS)

.PHONY: clean-c-deps
clean-c-deps:
	rm -rf $(C_DEPS_DIRS)
