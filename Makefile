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

# Variables to be overridden in the environment or on the command line, e.g.
#
#   GOFLAGS=-msan make build
GO      ?= go
GOFLAGS ?=

# Variables to be overridden on the command line only, e.g.
#
#   make test PKG=./pkg/storage TESTFLAGS=--vmodule=raft=1
PKG          := ./pkg/...
TAGS         :=
TESTS        := .
BENCHES      := -
TESTTIMEOUT  := 3m
RACETIMEOUT  := 10m
BENCHTIMEOUT := 5m
TESTFLAGS    :=
STRESSFLAGS  :=
DUPLFLAGS    := -t 100
COCKROACH    := ./cockroach
STARTFLAGS   := -s type=mem,size=1GiB --alsologtostderr
BUILDMODE    := install
BUILDTARGET  := .
SUFFIX       :=

# Possible values:
# <empty>: use the default toolchain
# release: target Linux 2.6.32, dynamically link to GLIBC 2.12.2
# musl:  target Linux 3.2.84, statically link musl 1.1.16
#
# Both release and musl only work in the cockroachdb/builder docker image,
# as they depend on cross-compilation toolchains available there.
#
# The release variant targets RHEL/CentOS 6.
#
# The musl variant targets the latest musl version (at the time of writing).
# The kernel version is the lowest available in combination with this version
# of musl. See:
# https://github.com/crosstool-ng/crosstool-ng/issues/540#issuecomment-276508500.
TYPE :=

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
export SHELL := $(shell which bash)
ifeq ($(SHELL),)
$(error bash is required)
endif
export GIT_PAGER :=

ifeq ($(TYPE),)
override LDFLAGS += -X github.com/cockroachdb/cockroach/pkg/build.typ=development
else ifeq ($(TYPE),release)
override LDFLAGS += -X github.com/cockroachdb/cockroach/pkg/build.typ=release
else ifeq ($(TYPE),musl)
# This tag disables jemalloc profiling. See https://github.com/jemalloc/jemalloc/issues/585.
override TAGS += musl
export CC  = /x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-gcc
export CXX = /x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-g++
override LDFLAGS += -extldflags -static -X github.com/cockroachdb/cockroach/pkg/build.typ=release-musl
override GOFLAGS += -installsuffix musl
else
$(error unknown build type $(TYPE))
endif

export MACOSX_DEPLOYMENT_TARGET=10.9

-include customenv.mk

.PHONY: all
all: build test check

.PHONY: short
short: build testshort checkshort

.PHONY: buildoss
buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss
buildoss: build

.PHONY: build
build: BUILDMODE = build -i -o cockroach$(SUFFIX)
build: install

.PHONY: start
start: build
start:
	$(COCKROACH) start $(STARTFLAGS)

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: install
install: override LDFLAGS += $(shell GOPATH=${GOPATH} build/ldflags.sh)
install:
	@echo "GOPATH set to $$GOPATH"
	@echo "$$GOPATH/bin added to PATH"
	$(GO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' $(BUILDTARGET)

# Build, but do not run the tests.
# PKG is expanded and all packages are built and moved to their directory.
.PHONY: testbuild
testbuild:
	$(GO) list -tags '$(TAGS)' -f \
	'$(GO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LDFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/{{.Name}}.test$(SUFFIX)' $(PKG) | \
	$(SHELL)

.PHONY: gotestdashi
gotestdashi:
	$(GO) test -v $(GOFLAGS) -tags '$(TAGS)' -i $(PKG)

.PHONY: test
test: gotestdashi
ifeq ($(BENCHES),-)
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -run "$(TESTS)" -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)
else
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -run "$(TESTS)" -bench "$(BENCHES)" -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)
endif

.PHONY: testshort
testshort: override TESTFLAGS += -short
testshort: test

testrace: override GOFLAGS += -race
testrace: TESTTIMEOUT := $(RACETIMEOUT)
testrace: test

.PHONY: testslow
testslow: override TESTFLAGS += -v
testslow: gotestdashi
ifeq ($(BENCHES),-)
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -run "$(TESTS)" -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10
else
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -run "$(TESTS)" -bench "$(BENCHES)" -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10
endif

.PHONY: testraceslow
testraceslow: override GOFLAGS += -race
testraceslow: TESTTIMEOUT := $(RACETIMEOUT)
testraceslow: testslow

# Beware! This target is complicated because it needs to handle complexity:
# - PKG may be specified as relative (e.g. './gossip') or absolute (e.g.
# github.com/cockroachdb/cockroach/gossip), and this target needs to create
# the test binary in the correct location and `cd` to the correct directory.
# This is handled by having `go list` produce the command line.
# - PKG may also be recursive (e.g. './pkg/...'). This is also handled by piping
# through `go list`.
# - PKG may not contain any tests! This is handled with an `if` statement that
# checks for the presence of a test binary before running `stress` on it.
.PHONY: stress
stress:
ifeq ($(BENCHES),-)
	$(GO) list -tags '$(TAGS)' -f '$(GO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LDFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/stress.test && (cd {{.Dir}} && if [ -f stress.test ]; then COCKROACH_STRESS=true stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)
else
	$(GO) list -tags '$(TAGS)' -f '$(GO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LDFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/stress.test && (cd {{.Dir}} && if [ -f stress.test ]; then COCKROACH_STRESS=true stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' -test.bench '\''$(BENCHES)'\'' -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)
endif

.PHONY: stressrace
stressrace: override GOFLAGS += -race
stressrace: TESTTIMEOUT := $(RACETIMEOUT)
stressrace: stress

# We're stuck copy-pasting this command from the test target because `ifeq`
# clauses are all executed by Make before any targets are ever run. That means
# that setting the BENCHES variable here doesn't effect the `ifeq` evaluation in
# the test target. Thus, if we were to simply set BENCHES and declare the test
# target as a prerequisite, we'd never actually run the benchmarks unless an
# override for BENCHES was specified on the command-line (which would take
# effect during `ifeq` processing). The alternative to copy-pasting would be to
# use shell-conditionals in the test target, which would quickly make the output
# of running `make test` or `make bench` very ugly.
# If golang/go#18010 is ever fixed, we can just get rid of all the `ifeq`s,
# always include -bench, and switch this back to specifying test as a prereq.
.PHONY: bench
bench: BENCHES := .
bench: TESTS := -
bench: TESTTIMEOUT := $(BENCHTIMEOUT)
bench: gotestdashi
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -run "$(TESTS)" -bench "$(BENCHES)" -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

.PHONY: upload-coverage
upload-coverage:
	$(GO) install ./vendor/github.com/wadey/gocovmerge
	$(GO) install ./vendor/github.com/mattn/goveralls
	@build/upload-coverage.sh

.PHONY: acceptance
acceptance:
	@pkg/acceptance/run.sh

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
check: override TAGS += check
check:
	$(GO) test ./build -v -tags '$(TAGS)' -run 'TestStyle/$(TESTS)'

.PHONY: checkshort
checkshort: override TAGS += check
checkshort:
	$(GO) test ./build -v -tags '$(TAGS)' -short -run 'TestStyle/$(TESTS)'

.PHONY: clean
clean:
	$(GO) clean $(GOFLAGS) -i github.com/cockroachdb/...
	find . -name '*.test*' -type f -exec rm -f {} \;
	rm -f .bootstrap

.PHONY: protobuf
protobuf:
	$(MAKE) -C .. -f cockroach/build/protobuf.mk

# The .go-version target is phony so that it is rebuilt every time.
.PHONY: .go-version
.go-version:
	@actual=$$($(GO) version); \
	echo "$${actual}" | grep -q -E '\b$(GOVERS)\b' || \
	  (echo "$(GOVERS) required (see CONTRIBUTING.md): $${actual}" && false)

include .go-version

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

GLOCK := ../../../../bin/glock
#        ^  ^  ^  ^~ GOPATH
#        |  |  |~ GOPATH/src
#        |  |~ GOPATH/src/github.com
#        |~ GOPATH/src/github.com/cockroachdb

# Update the git hooks and run the bootstrap script whenever any
# of them (or their dependencies) change.
.bootstrap: $(GITHOOKS) GLOCKFILE glide.lock
	git submodule update --init
	$(GO) install ./vendor/github.com/robfig/glock
	@unset GIT_WORK_TREE; $(GLOCK) sync -n < GLOCKFILE
	touch $@

# Force Make to run the .bootstrap recipe before building any other targets.
-include .bootstrap
