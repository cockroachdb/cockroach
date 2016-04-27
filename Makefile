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
#
# Author: Andrew Bonventre (andybons@gmail.com)
# Author: Shawn Morel (shawnmorel@gmail.com)
# Author: Spencer Kimball (spencer.kimball@gmail.com)

# Cockroach build rules.
GO ?= go
# Allow setting of go build flags from the command line.
GOFLAGS :=
# Set to 1 to use static linking for all builds (including tests).
STATIC :=

# Variables to be overridden on the command line, e.g.
#   make test PKG=./storage TESTFLAGS=--vmodule=raft=1
PKG          := ./...
TAGS         :=
TESTS        := .
TESTTIMEOUT  := 1m10s
RACETIMEOUT  := 5m
BENCHTIMEOUT := 5m
TESTFLAGS    :=
STRESSFLAGS  := -stderr -maxfails 1
DUPLFLAGS    := -t 100
BUILDMODE    := install
CURRENTDIR   := $(realpath .)
export GOPATH := $(realpath ../../../..)
# Prefer tools from $GOPATH/bin over those elsewhere on the path.
# This ensures that we get the versions pinned in the GLOCKFILE.
export PATH := $(GOPATH)/bin:$(PATH)
# HACK: Make has a fast path and a slow path for command execution,
# but the fast path uses the PATH variable from when make was started,
# not the one we set on the previous line. In order for the above
# line to have any effect, we must force make to always take the slow path.
# Setting the SHELL variable to a value other than the default (/bin/sh)
# is one way to do this globally.
# http://stackoverflow.com/questions/8941110/how-i-could-add-dir-to-path-in-makefile/13468229#13468229
SHELL := $(shell which bash)
export GIT_PAGER :=

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.

ifeq ($(STATIC),1)
# Static linking with glibc is a bad time; see
# https://github.com/golang/go/issues/13470. If a static build is
# requested, only link libgcc and libstdc++ statically.
# TODO(peter): Allow this only when `go env CC` reports "gcc".
LDFLAGS += -extldflags "-static-libgcc -static-libstdc++"
endif

.PHONY: all
all: build test check

.PHONY: release
release: build

# The uidebug build tag is used to turn off embedding of UI assets into the
# cockroach binary, loading them from the local filesystem at run time instead.
# This build target is intended for use by UI developers, as it provides a
# faster iteration cycle which doesn't require recompilation of the binary.
.PHONY: uidebug
uidebug: TAGS += uidebug
uidebug: build

.PHONY: build
build: GOFLAGS += -i -o cockroach
build: BUILDMODE = build
build: install

.PHONY: install
install: LDFLAGS += $(shell GOPATH=${GOPATH} build/ldflags.sh)
install:
	@echo "GOPATH set to $$GOPATH"
	@echo "$$GOPATH/bin added to PATH"
	@echo $(GO) $(BUILDMODE) -v $(GOFLAGS)
	@$(GO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)'

# Build, but do not run the tests.
# PKG is expanded and all packages are built and moved to their directory.
# If STATIC=1, tests are statically linked.
# eg: to statically build the sql tests, run:
#   make testbuild PKG=./sql STATIC=1
.PHONY: testbuild
testbuild: GOFLAGS += -c
testbuild:
	for p in $(shell $(GO) list -tags '$(TAGS)' $(PKG)); do \
	  NAME=$$(basename "$$p"); \
	  OUT="$$NAME.test"; \
	  DIR=$$($(GO) list -f {{.Dir}} -tags '$(TAGS)' $$p); \
	  $(GO) test -v $(GOFLAGS) -tags '$(TAGS)' -o "$$DIR"/"$$OUT" -ldflags '$(LDFLAGS)' "$$p" $(TESTFLAGS) || exit 1; \
	done

# Build all tests into DIR and strips each.
# DIR is required.
.PHONY: testbuildall
testbuildall: GOFLAGS += -c
testbuildall:
ifndef DIR
	$(error DIR is undefined)
endif
	for p in $(shell $(GO) list $(PKG)); do \
	  NAME=$$(basename "$$p"); \
	  PKGDIR=$$($(GO) list -f {{.ImportPath}} $$p); \
	  OUTPUT_FILE="$(DIR)/$${PKGDIR}/$${NAME}.test"; \
	  $(GO) test -v $(GOFLAGS) -o $${OUTPUT_FILE} -ldflags '$(LDFLAGS)' "$$p" $(TESTFLAGS) || exit 1; \
	  if [ -s $${OUTPUT_FILE} ]; then strip -S $${OUTPUT_FILE}; fi; \
	  if [ $${NAME} = "sql" ]; then \
	     cp -r sql/testdata sql/partestdata "$(DIR)/$${PKGDIR}/" || exit 1; \
	  fi \
	done

# Similar to "testrace", we want to cache the build before running the
# tests.
.PHONY: test
test:
	$(GO) test -v $(GOFLAGS) -i $(PKG)
	$(GO) test $(GOFLAGS) -run $(TESTS) -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

.PHONY: testslow
testslow: TESTFLAGS += -v
testslow:
	$(GO) test -v $(GOFLAGS) -i $(PKG)
	$(GO) test $(GOFLAGS) -run $(TESTS) -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10

.PHONY: testraceslow
testraceslow: TESTFLAGS += -v
testraceslow:
	$(GO) test -v $(GOFLAGS) -i $(PKG)
	$(GO) test $(GOFLAGS) -race -run $(TESTS) -timeout $(RACETIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10

# "go test -i" builds dependencies and installs them into GOPATH/pkg, but does not run the
# tests. Run it as a part of "testrace" since race-enabled builds are not covered by
# "make build", and so they would be built from scratch every time (including the
# slow-to-compile cgo packages).
.PHONY: testrace
testrace:
	$(GO) test -v $(GOFLAGS) -race -i $(PKG)
	$(GO) test $(GOFLAGS) -race -run $(TESTS) -timeout $(RACETIMEOUT) $(PKG) $(TESTFLAGS)

.PHONY: bench
bench:
	$(GO) test -v $(GOFLAGS) -i $(PKG)
	$(GO) test $(GOFLAGS) -run - -bench $(TESTS) -timeout $(BENCHTIMEOUT) $(PKG) $(TESTFLAGS)

.PHONY: coverage
coverage:
	$(GO) test -v $(GOFLAGS) -i $(PKG)
	$(GO) test $(GOFLAGS) -cover -run $(TESTS) $(PKG) $(TESTFLAGS)

# "make stress PKG=./storage TESTS=TestBlah" will build the given test
# and run it in a loop (the PKG argument is required; if TESTS is not
# given all tests in the package will be run).
.PHONY: stress
stress:
	$(GO) test -v $(GOFLAGS) -i -c $(PKG) -o stress.test
	cd $(PKG) && stress $(STRESSFLAGS) $(CURRENTDIR)/stress.test -test.run $(TESTS) -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

.PHONY: stressrace
stressrace:
	$(GO) test $(GOFLAGS) -race -v -i -c $(PKG) -o stress.test
	cd $(PKG) && stress $(STRESSFLAGS) $(CURRENTDIR)/stress.test -test.run $(TESTS) -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

.PHONY: acceptance
acceptance:
	@acceptance/run.sh

.PHONY: dupl
dupl:
	find . -name '*.go'             \
	       -not -name '*.pb.go'     \
	       -not -name '*.pb.gw.go'  \
	       -not -name 'embedded.go' \
	       -not -name '*_string.go' \
	       -not -name 'sql.go'      \
	| dupl -files $(DUPLFLAGS)

.PHONY: check
check:
	@build/check-style.sh

.PHONY: clean
clean:
	$(GO) clean $(GOFLAGS) -i github.com/cockroachdb/...
	find . -name '*.test' -type f -exec rm -f {} \;
	rm -f .bootstrap
	make -C ui clean

.PHONY: protobuf
protobuf:
	$(MAKE) -C .. -f cockroach/build/protobuf.mk

# The .go-version target is phony so that it is rebuilt every time.
.PHONY: .go-version
.go-version:
	@actual=$$($(GO) version); \
	echo "$${actual}" | grep -q '\b$(GOVERS)\b' || \
	  (echo "$(GOVERS) required (see CONTRIBUTING.md): $${actual}" && false)

include .go-version

ifneq ($(SKIP_BOOTSTRAP),1)

GITHOOKS := $(subst githooks/,.git/hooks/,$(wildcard githooks/*))
.git/hooks/%: githooks/%
	@echo installing $<
	@rm -f $@
	@mkdir -p $(dir $@)
	@ln -s ../../$(basename $<) $(dir $@)

GLOCK := ../../../../bin/glock
#        ^  ^  ^  ^~ GOPATH
#        |  |  |~ GOPATH/src
#        |  |~ GOPATH/src/github.com
#        |~ GOPATH/src/github.com/cockroachdb

$(GLOCK):
	$(GO) get github.com/robfig/glock

# Update the git hooks and run the bootstrap script whenever any
# of them (or their dependencies) change.
.bootstrap: $(GITHOOKS) $(GLOCK) GLOCKFILE
	@unset GIT_WORK_TREE; $(GLOCK) sync github.com/cockroachdb/cockroach
	touch $@

include .bootstrap

endif
