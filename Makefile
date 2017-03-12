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
XGO     ?= xgo
TAR     ?= tar

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
ARCHIVE      := cockroach.src.tgz
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

# GNU tar and BSD tar both support transforming filenames according to a regular
# expression, but have different flags to do so.
TAR_XFORM_FLAG = $(shell $(TAR) --version | grep -q GNU && echo "--xform=s" || echo "-s")

GIT_DIR := $(shell git rev-parse --git-dir 2> /dev/null)

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

.PHONY: xgo-build
xgo-build: GO = $(XGO)
xgo-build: BUILDMODE =
xgo-build: install

.PHONY: start
start: build
start:
	$(COCKROACH) start $(STARTFLAGS)

.PHONY: install
# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
install: override LDFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)"
# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
install: .buildinfo/tag .buildinfo/rev
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
	rm -f .bootstrap $(ARCHIVE)

.PHONY: protobuf
protobuf:
	$(MAKE) -C .. -f cockroach/build/protobuf.mk

# archive builds a source tarball out of this repository. All files in the
# archive are prefixed with $(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach,
# with the exception of files in build/archive, which are instead installed
# directly into $(ARCHIVE_BASE).
.PHONY: archive
archive: $(ARCHIVE)

$(ARCHIVE): $(ARCHIVE).tmp
	gzip -c $< > $@

.INTERMEDIATE: $(ARCHIVE).tmp
$(ARCHIVE).tmp: ARCHIVE_BASE = cockroach-$(shell cat .buildinfo/tag)
$(ARCHIVE).tmp: .buildinfo/tag .buildinfo/rev
	scripts/ls-files.sh | $(TAR) -cf $@ -T - $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, $^
	(cd build/archive && $(TAR) -rf ../../$@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/, --exclude README.md *)

.buildinfo:
	@mkdir -p $@

.buildinfo/tag: | .buildinfo
	@{ git describe --tags --exact-match 2> /dev/null || git rev-parse --short HEAD; } | tr -d \\n > $@
	@git diff-index --quiet HEAD || echo -dirty >> $@

.buildinfo/rev: | .buildinfo
	@git rev-parse HEAD > $@

ifneq ($(GIT_DIR),)
# If we're in a Git checkout, we update the buildinfo information on every build
# to keep it up-to-date.
.buildinfo/tag: .ALWAYS_REBUILD
.buildinfo/rev: .ALWAYS_REBUILD
endif

# The .go-version target is phony so that it is rebuilt every time.
.PHONY: .go-version
.go-version:
	@actual=$$($(GO) version); \
	echo "$${actual}" | grep -q -E '\b$(GOVERS)\b' || \
	  (echo "$(GOVERS) required (see CONTRIBUTING.md): $${actual}" && false)

include .go-version

ifneq ($(GIT_DIR),)
# If we're in a git worktree, the git hooks directory may not be in our root,
# so we ask git for the location.
#
# Note that `git rev-parse --git-path hooks` requires git 2.5+.
GITHOOKSDIR := $(shell git rev-parse --git-path hooks 2> /dev/null || echo "$(GIT_DIR)/hooks")
GITHOOKS := $(subst githooks/,$(GITHOOKSDIR)/,$(wildcard githooks/*))
$(GITHOOKSDIR)/%: githooks/%
	@echo installing $<
	@rm -f $@
	@mkdir -p $(dir $@)
	@ln -s ../../$(basename $<) $(dir $@)
endif

GLOCK := ../../../../bin/glock
#        ^  ^  ^  ^~ GOPATH
#        |  |  |~ GOPATH/src
#        |  |~ GOPATH/src/github.com
#        |~ GOPATH/src/github.com/cockroachdb

# Update the git hooks and run the bootstrap script whenever any
# of them (or their dependencies) change.
.bootstrap: $(GITHOOKS) GLOCKFILE glide.lock
ifneq ($(GIT_DIR),)
	git submodule update --init
endif
	$(GO) install ./vendor/github.com/robfig/glock
	@unset GIT_WORK_TREE; $(GLOCK) sync -n < GLOCKFILE
	touch $@

# Force Make to run the .bootstrap recipe before building any other targets.
-include .bootstrap
