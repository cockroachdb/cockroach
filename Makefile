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

# Variables to be overridden on the command line only, e.g.
#
#   make test PKG=./pkg/storage TESTFLAGS=--vmodule=raft=1
#
# Note that environment variable overrides are intentionally ignored.
PKG          := ./pkg/...
TAGS         :=
TESTS        := .
BENCHES      :=
FILES        :=
TESTTIMEOUT  := 4m
RACETIMEOUT  := 15m
BENCHTIMEOUT := 5m
TESTFLAGS    :=
STRESSFLAGS  :=
DUPLFLAGS    := -t 100
GOFLAGS      :=
ARCHIVE      := cockroach.src.tgz
STARTFLAGS   := -s type=mem,size=1GiB --logtostderr
BUILDMODE    := install
BUILDTARGET  := .
SUFFIX       :=
INSTALL      := install
prefix       := /usr/local
bindir       := $(prefix)/bin

REPO_ROOT := .
include $(REPO_ROOT)/build/common.mk

# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
ifdef HOST_MACOS
# The default MACOSX_DEPLOYMENT_TARGET uses only the major and minor
# version numbers, not the patch number. The linker will emit a warning if
# e.g. we try to link a library built for 10.12.4 while the
# MACOSX_DEPLOYMENT_TARGET is 10.12.
export MACOSX_DEPLOYMENT_TARGET ?= $(shell sw_vers -productVersion | grep -oE '\d+\.\d+')
endif

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
LINKFLAGS ?=

# TYPE=release builds a release build, which omit debug information to produce a
# smaller binary.
TYPE :=
ifeq ($(TYPE),)
else ifeq ($(TYPE),release)
override LINKFLAGS += -s -w -X github.com/cockroachdb/cockroach/pkg/build.typ=$(BUILD_TYPE)
else
$(error unknown build type $(TYPE))
endif

# Go's default of using `$GOOS_$GOARCH[_race][_msan]` to isolate compilation
# artifacts for different platforms isn't sufficient. We also want to isolate
# artifacts from our stdmalloc and deadlock builds, for example, and by compiler
# when targeting e.g. different versions of glibc on the same machine.
# Unfortunately, attempting ot use -installsuffix for this purpose (which is
# literally the point of -installsuffix, to be clear) causes Go to attempt to
# write some core packages to $GOROOT, where it often doesn't have write access.
#
# Since we're already constructing a NATIVE_TAG to isolate artifacts from C and
# C++ dependencies, just use that same tag to construct our own pkgdir. Note
# that -pkgdir completely overrides the automatic -installsuffix, so we're
# careful to include _race and _msan in NATIVE_TAG when GOFLAGS contains -race
# or -msan, respectively.
override GOFLAGS += -pkgdir $(GOPATH)/pkg/$(NATIVE_TAG)
override TAGS += make $(NATIVE_TAG)

XGO := $(strip $(if $(XGOOS),GOOS=$(XGOOS)) $(if $(XGOARCH),GOARCH=$(XGOARCH)) $(if $(XHOST_TRIPLE),CC=$(CC_PATH) CXX=$(CXX_PATH)) $(GO))

COCKROACH := ./cockroach$(SUFFIX)$$($(XGO) env GOEXE)

.DEFAULT_GOAL := all
all: $(COCKROACH)

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss

$(COCKROACH) build buildoss: BUILDMODE = build -i -o $(COCKROACH)

# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
$(COCKROACH) build buildoss go-install: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)"

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: build buildoss install
$(COCKROACH) build buildoss go-install: $(C_LIBS) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET) .buildinfo/tag .buildinfo/rev
	 $(XGO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

.PHONY: install
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
gotestdashi: $(C_LIBS) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET)
	$(XGO) test -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i $(PKG)

testshort: override TESTFLAGS += -short

testrace: override GOFLAGS += -race
testrace: export GORACE := halt_on_error=1
testrace: TESTTIMEOUT := $(RACETIMEOUT)

# Directory scans in the builder image are excruciatingly slow when running
# Docker for Mac, so we filter out the 20k+ UI dependencies that are
# guaranteed to be irrelevant to save nearly 10s on every Make invocation.
FIND_RELEVANT := find pkg -name node_modules -prune -o

bin/sql.test: main.go $(shell $(FIND_RELEVANT) ! -name 'zcgo_flags.go' -name '*.go')
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o bin/sql.test ./pkg/sql

bench: BENCHES := .
bench: TESTS := -
bench: TESTTIMEOUT := $(BENCHTIMEOUT)

.PHONY: check test testshort testrace testlogic bench
check test testshort testrace bench: gotestdashi
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

# Run make testlogic to run all of the logic tests. Specify test files to run
# with make testlogic FILES="foo bar".
testlogic: TESTS := $(if $(FILES),TestLogic$$//^$(subst $(space),$$|^,$(FILES))$$,TestLogic)
testlogic: TESTFLAGS := -test.v $(if $(FILES),-show-sql)
testlogic: bin/sql.test
	cd pkg/sql && sql.test -test.run "$(TESTS)" -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

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
stress stressrace: $(C_LIBS) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET)
	$(GO) list -tags '$(TAGS)' -f '$(XGO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o '\''{{.Dir}}'\''/stress.test && (cd '\''{{.Dir}}'\'' && if [ -f stress.test ]; then COCKROACH_STRESS=true stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' $(if $(BENCHES),-test.bench '\''$(BENCHES)'\'') -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)

.PHONY: upload-coverage
upload-coverage: $(BOOTSTRAP_TARGET)
	$(GO) install ./vendor/github.com/wadey/gocovmerge
	$(GO) install ./vendor/github.com/mattn/goveralls
	@build/upload-coverage.sh

.PHONY: acceptance
acceptance:
	@pkg/acceptance/run.sh

.PHONY: dupl
dupl: $(BOOTSTRAP_TARGET)
	$(FIND_RELEVANT) \
	       -name '*.go'             \
	       -not -name '*.pb.go'     \
	       -not -name '*.pb.gw.go'  \
	       -not -name 'embedded.go' \
	       -not -name '*_string.go' \
	       -not -name 'sql.go'      \
	| dupl -files $(DUPLFLAGS)

# All packages need to be installed before we can run (some) of the checks and
# code generators reliably. More precisely, anything that uses x/tools/go/loader
# is fragile (this includes stringer, vet and others). The blocking issue is
# https://github.com/golang/go/issues/14120.

# `go generate` uses stringer and so must depend on gotestdashi per the above
# comment. See https://github.com/golang/go/issues/10249 for details.
.PHONY: generate
generate: gotestdashi
	$(GO) generate $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(PKG)

# The style checks depend on `go vet` and so must depend on gotestdashi per the
# above comment. See https://github.com/golang/go/issues/16086 for details.
.PHONY: lint
lint: override TAGS += lint
lint: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run 'TestStyle/$(TESTS)'

.PHONY: lintshort
lintshort: override TAGS += lint
lintshort: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -run 'TestStyle/$(TESTS)'

.PHONY: clean
clean: clean-c-deps
	$(GO) clean $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i github.com/cockroachdb/...
	$(FIND_RELEVANT) -type f \( -name 'zcgo_flags*.go' -o -name '*.test' \) -exec rm {} +
	rm -f $(BOOTSTRAP_TARGET) $(ARCHIVE)

.PHONY: protobuf
protobuf:
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

# pre-push locally runs most of the checks CI will run. Notably, it doesn't run
# the acceptance tests.
.PHONY: pre-push
pre-push: generate lint test
	$(MAKE) -C $(REPO_ROOT)/pkg/ui lint test
	! git status --porcelain | read || (git status; git --no-pager diff -a 1>&2; exit 1)

# archive builds a source tarball out of this repository. Files in the special
# directory build/archive/contents are inserted directly into $(ARCHIVE_BASE).
# All other files in the repository are inserted into the archive with prefix
# $(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach to allow the extracted
# archive to serve directly as a GOPATH root.
.PHONY: archive
archive: $(ARCHIVE)

$(ARCHIVE): $(ARCHIVE).tmp
	gzip -c $< > $@

# TODO(benesch): Make this recipe use `git ls-files --recurse-submodules`
# instead of scripts/ls-files.sh once Git v2.11 is widely deployed.
.INTERMEDIATE: $(ARCHIVE).tmp
$(ARCHIVE).tmp: ARCHIVE_BASE = cockroach-$(shell cat .buildinfo/tag)
$(ARCHIVE).tmp: .buildinfo/tag .buildinfo/rev
	scripts/ls-files.sh | $(TAR) -cf $@ -T - $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, $^
	(cd build/archive/contents && $(TAR) -rf ../../../$@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/, *)

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
