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
TESTTIMEOUT  := 3m
RACETIMEOUT  := 15m
BENCHTIMEOUT := 5m
TESTFLAGS    :=
STRESSFLAGS  :=
DUPLFLAGS    := -t 100
COCKROACH    := ./cockroach
ARCHIVE      := cockroach.src.tgz
STARTFLAGS   := -s type=mem,size=1GiB --logtostderr
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

# We intentionally use LINKFLAGS instead of the more traditional LDFLAGS
# because LDFLAGS has built-in semantics that don't make sense with the Go
# toolchain.
LINKFLAGS ?=

ifeq ($(TYPE),)
override LINKFLAGS += -X github.com/cockroachdb/cockroach/pkg/build.typ=development
else ifeq ($(TYPE),release)
override LINKFLAGS += -s -w -X github.com/cockroachdb/cockroach/pkg/build.typ=release
else ifeq ($(TYPE),musl)
# This tag disables jemalloc profiling. See https://github.com/jemalloc/jemalloc/issues/585.
override TAGS += musl
export CC  = /x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-gcc
export CXX = /x-tools/x86_64-unknown-linux-musl/bin/x86_64-unknown-linux-musl-g++
override LINKFLAGS += -extldflags -static -X github.com/cockroachdb/cockroach/pkg/build.typ=release-musl
override GOFLAGS += -installsuffix musl
else
$(error unknown build type $(TYPE))
endif

export MACOSX_DEPLOYMENT_TARGET=10.9

REPO_ROOT := .
include $(REPO_ROOT)/build/common.mk

.DEFAULT_GOAL := all
.PHONY: all
all: build test check

.PHONY: short
short: build testshort checkshort

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss

build buildoss: BUILDMODE = build -i -o cockroach$(SUFFIX)

xgo-build: GO = $(XGO)
xgo-build: BUILDMODE =

# Special case: we install the cockroach binary to the user-specified GOBIN
# (usually GOPATH/bin) rather than our repository-local GOBIN.
install: GOBIN := $(ORIGINAL_GOBIN)

.PHONY: build buildoss xgo-build install
# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
build buildoss xgo-build install: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)"
# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
build buildoss xgo-build install: $(BOOTSTRAP_TARGET) .buildinfo/tag .buildinfo/rev
	$(GO) $(BUILDMODE) -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(BUILDTARGET)

.PHONY: start
start: build
start:
	$(COCKROACH) start $(STARTFLAGS)

# Build, but do not run the tests.
# PKG is expanded and all packages are built and moved to their directory.
.PHONY: testbuild
testbuild: $(BOOTSTRAP_TARGET)
	$(GO) list -tags '$(TAGS)' -f \
	'$(GO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o {{.Dir}}/{{.Name}}.test$(SUFFIX)' $(PKG) | \
	$(SHELL)

.PHONY: gotestdashi
gotestdashi: $(BOOTSTRAP_TARGET)
	$(GO) test -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i $(PKG)

testshort: override TESTFLAGS += -short

testrace: override GOFLAGS += -race
testrace: GORACE := halt_on_error=1
testrace: TESTTIMEOUT := $(RACETIMEOUT)

# Run make testlogic to run all of the logic tests. Specify test files to run
# with make testlogic FILES="foo bar".
testlogic: PKG := ./pkg/sql
testlogic: TESTS := $(if $(FILES),TestLogic$$//^$(subst $(space),$$|^,$(FILES))$$,TestLogic)
testlogic: TESTFLAGS := -v $(if $(FILES),-show-sql)

bench: BENCHES := .
bench: TESTS := -
bench: TESTTIMEOUT := $(BENCHTIMEOUT)

.PHONY: test testshort testrace testlogic bench
test testshort testrace testlogic bench: gotestdashi
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

testraceslow: override GOFLAGS += -race
testraceslow: TESTTIMEOUT := $(RACETIMEOUT)

.PHONY: testslow testraceslow
testslow testraceslow: override TESTFLAGS += -v
testslow testraceslow: gotestdashi
	$(GO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS) | grep -F ': Test' | sed -E 's/(--- PASS: |\(|\))//g' | awk '{ print $$2, $$1 }' | sort -rn | head -n 10

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
stress stressrace: $(BOOTSTRAP_TARGET)
	$(GO) list -tags '$(TAGS)' -f '$(GO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o '\''{{.Dir}}'\''/stress.test && (cd '\''{{.Dir}}'\'' && if [ -f stress.test ]; then COCKROACH_STRESS=true stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' $(if $(BENCHES),-test.bench '\''$(BENCHES)'\'') -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)

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
	find . -name '*.go'             \
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
	$(GO) generate $(PKG)

# The style checks depend on `go vet` and so must depend on gotestdashi per the
# above comment. See https://github.com/golang/go/issues/16086 for details.
.PHONY: check
check: override TAGS += check
check: gotestdashi
	$(GO) test ./build -v -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run 'TestStyle/$(TESTS)'

.PHONY: checkshort
checkshort: override TAGS += check
checkshort: gotestdashi
	$(GO) test ./build -v -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -run 'TestStyle/$(TESTS)'

.PHONY: clean
clean:
	$(GO) clean $(GOFLAGS) -i github.com/cockroachdb/...
	find . -name '*.test*' -type f -exec rm -f {} \;
	rm -f .bootstrap $(ARCHIVE)

.PHONY: protobuf
protobuf:
	$(MAKE) -C .. -f cockroach/build/protobuf.mk

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
$(ARCHIVE).tmp: $(BOOTSTRAP_TARGET) .buildinfo/tag .buildinfo/rev
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
