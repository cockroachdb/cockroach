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
XGOFLAGS     :=
COCKROACH    := ./cockroach
ARCHIVE      := cockroach.src.tgz
STARTFLAGS   := -s type=mem,size=1GiB --logtostderr
BUILDMODE    := install
BUILDTARGET  := .
SUFFIX       :=

# Possible values:
# <empty>: use the default toolchain
# release-linux-gnu:  target Linux 2.6.32, dynamically link GLIBC 2.12.2
# release-linux-musl: target Linux 2.6.32, statically link musl 1.1.16
# release-darwin:     target OS X 10.9
# release-windows:    target Windows 8, statically link all non-Windows libraries
#
# All non-empty variants only work in the cockroachdb/builder docker image, as
# they depend on cross-compilation toolchains available there.
TYPE :=

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
LINKFLAGS ?=

ifeq ($(TYPE),)
override LINKFLAGS += -X github.com/cockroachdb/cockroach/pkg/build.typ=development
# Temporary shim to prevent build pollution in CI, which may still have agents
# with caches populated with the release toolchain.
#
# TODO(tamird): replace with `else ifeq ($(TYPE),release-linux-gnu)` when
# sufficient time has passed.
else ifeq ($(TYPE),$(filter $(TYPE),release release-linux-gnu))
# We use a custom toolchain to target old Linux and glibc versions. However,
# this toolchain's libstdc++ version is quite recent and must be statically
# linked to avoid depending on the target's available libstdc++.
XHOST_TRIPLE := x86_64-unknown-linux-gnu
override LINKFLAGS += -s -w -extldflags "-static-libgcc -static-libstdc++" -X github.com/cockroachdb/cockroach/pkg/build.typ=release
override GOFLAGS += -installsuffix release
else ifeq ($(TYPE),release-linux-musl)
XHOST_TRIPLE := x86_64-unknown-linux-musl
override LINKFLAGS += -s -w -extldflags "-static" -X github.com/cockroachdb/cockroach/pkg/build.typ=release-musl
override GOFLAGS += -installsuffix release-musl
override SUFFIX := $(SUFFIX)-linux-2.6.32-musl-amd64
else ifeq ($(TYPE),release-darwin)
XGOOS := darwin
export CGO_ENABLED := 1
XHOST_TRIPLE := x86_64-apple-darwin13
override SUFFIX := $(SUFFIX)-darwin-10.9-amd64
override LINKFLAGS += -s -w -X github.com/cockroachdb/cockroach/pkg/build.typ=release
else ifeq ($(TYPE),release-windows)
XGOOS := windows
export CGO_ENABLED := 1
XHOST_TRIPLE := x86_64-w64-mingw32
override SUFFIX := $(SUFFIX)-windows-6.2-amd64
override LINKFLAGS += -s -w -extldflags "-static" -X github.com/cockroachdb/cockroach/pkg/build.typ=release
else
$(error unknown build type $(TYPE))
endif

REPO_ROOT := .
include $(REPO_ROOT)/build/common.mk
override TAGS += make $(TARGET_TRIPLE_TAG)

# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
ifdef MACOS
export MACOSX_DEPLOYMENT_TARGET ?= $(shell sw_vers -productVersion)
endif

.DEFAULT_GOAL := all
.PHONY: all
all: build test check

.PHONY: short
short: build testshort checkshort

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss

build buildoss: BUILDMODE = build -i -o cockroach$(SUFFIX)

# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
build buildoss install: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)"

XGO := $(if $(XGOOS),GOOS=$(XGOOS)) $(if $(XGOARCH),GOARCH=$(XGOARCH)) $(if $(XHOST_TRIPLE),CC=$(CC_PATH) CXX=$(CXX_PATH)) $(GO)

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: build buildoss install
build buildoss install: $(C_LIBS) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET) .buildinfo/tag .buildinfo/rev
	 $(XGO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

.PHONY: start
start: build
start:
	$(COCKROACH) start $(STARTFLAGS)

# Build, but do not run the tests.
# PKG is expanded and all packages are built and moved to their directory.
.PHONY: testbuild
testbuild: gotestdashi
	$(XGO) list -tags '$(TAGS)' -f \
	'$(XGO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/{{.Name}}.test$(SUFFIX)' $(PKG) | \
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

.PHONY: test testshort testrace testlogic bench
test testshort testrace bench: gotestdashi
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
.PHONY: check
check: override TAGS += check
check: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run 'TestStyle/$(TESTS)'

.PHONY: checkshort
checkshort: override TAGS += check
checkshort: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -run 'TestStyle/$(TESTS)'

.PHONY: clean
clean: clean-c-deps
	$(GO) clean $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i github.com/cockroachdb/...
	$(FIND_RELEVANT) -type f \( -name 'zcgo_flags*.go' -o -name '*.test' \) -exec rm {} +
	rm -f $(BOOTSTRAP_TARGET) $(ARCHIVE)

.PHONY: protobuf
protobuf:
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

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
