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

# Variables to be overridden on the command line only, e.g.
#
#   make test PKG=./pkg/storage TESTFLAGS=--vmodule=raft=1
#
# Note that environment variable overrides are intentionally ignored.

# Comments starting with a double-hash (##) are for self-documentation, see the
# `help` target. They look a bit awkward in the variable declarations below
# since any whitespace added would become part of the variable's default value.

PKG          := ./pkg/...## Which package to run tests against, e.g. "./pkg/storage".
TAGS         :=
TESTS        :=.## Tests to run for use with `make test`.
BENCHES      :=## Benchmarks to run for use with `make bench`.
FILES        :=## Space delimited list of logic test files to run, for make testlogic.
TESTTIMEOUT  := 4m## Test timeout to use for regular tests.
RACETIMEOUT  := 15m## Test timeout to use for race tests.
ACCEPTANCETIMEOUT := 30m## Test timeout to use for acceptance tests.
BENCHTIMEOUT := 5m## Test timeout to use for benchmarks.
TESTFLAGS    :=## Extra flags to pass to the go test runner, e.g. "-v --vmodule=raft=1"
STRESSFLAGS  :=## Extra flags to pass to `stress` during `make stress`.
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
MAKEFLAGS += $(shell $(REPO_ROOT)/build/jflag.sh)

help: ## Print help for targets with comments.
	@echo "Usage:"
	@echo "  make [target...] [VAR=foo VAR2=bar...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(shell tput setaf 6 2>/dev/null)%-30s$(shell tput sgr0 2>/dev/null) %s\n", $$1, $$2}'
	@echo ""
	@echo "Useful variables:"
	@grep -Eh '^[a-zA-Z._-]+ *:=.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":=.*?## "}; {printf "  $(shell tput setaf 6 2>/dev/null)%-30s$(shell tput sgr0 2>/dev/null) %s\n", $$1, $$2}'

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

include $(REPO_ROOT)/build/common.mk
override TAGS += make $(NATIVE_SPECIFIER_TAG)

# On macOS 10.11, XCode SDK v8.1 (and possibly others) indicate the presence of
# symbols that don't exist until macOS 10.12. Setting MACOSX_DEPLOYMENT_TARGET
# to the host machine's actual macOS version works around this. See:
# https://github.com/jemalloc/jemalloc/issues/494.
ifdef MACOS
export MACOSX_DEPLOYMENT_TARGET ?= $(shell sw_vers -productVersion | grep -oE '[0-9]+\.[0-9]+')
endif

XGO := $(strip $(if $(XGOOS),GOOS=$(XGOOS)) $(if $(XGOARCH),GOARCH=$(XGOARCH)) $(if $(XHOST_TRIPLE),CC=$(CC_PATH) CXX=$(CXX_PATH)) $(GO))

COCKROACH := ./cockroach$(SUFFIX)$(shell $(XGO) env GOEXE)

.DEFAULT_GOAL := all
all: $(COCKROACH)

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss
buildoss: $(C_LIBS_OSS)

$(COCKROACH) build go-install: $(C_LIBS_CCL)

$(COCKROACH) build buildoss: BUILDMODE = build -i -o $(COCKROACH)

# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
$(COCKROACH) build buildoss go-install check test testshort testrace bench: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.baseBranch=$(shell cat .buildinfo/basebranch)"

# Note: We pass `-v` to `go build` and `go test -i` so that warnings
# from the linker aren't suppressed. The usage of `-v` also shows when
# dependencies are rebuilt which is useful when switching between
# normal and race test builds.
.PHONY: build buildoss install
build: ## Build the CockroachDB binary.
buildoss: ## Build the CockroachDB binary without any CCL-licensed code.
$(COCKROACH) build buildoss go-install: $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET) .buildinfo/tag .buildinfo/rev .buildinfo/basebranch
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
gotestdashi: $(C_LIBS_CCL) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET)
	$(XGO) test -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i $(PKG)

testshort: override TESTFLAGS += -short

testrace: ## Run tests with the Go race detector enabled.
testrace: override GOFLAGS += -race
testrace: export GORACE := halt_on_error=1
testrace: TESTTIMEOUT := $(RACETIMEOUT)

# Directory scans in the builder image are excruciatingly slow when running
# Docker for Mac, so we filter out the 20k+ UI dependencies that are
# guaranteed to be irrelevant to save nearly 10s on every Make invocation.
FIND_RELEVANT := find pkg -name node_modules -prune -o

bin/logictest.test: PKG := ./pkg/sql/logictest
bin/logictest.test: main.go $(shell $(FIND_RELEVANT) ! -name 'zcgo_flags.go' -name '*.go')
	$(MAKE) gotestdashi GOFLAGS='$(GOFLAGS)' TAGS='$(TAGS)' LINKFLAGS='$(LINKFLAGS)' PKG='$(PKG)'
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o bin/logictest.test $(PKG)

bench: ## Run benchmarks.
bench: BENCHES := .
bench: TESTS := -
bench: TESTTIMEOUT := $(BENCHTIMEOUT)

.PHONY: check test testshort testrace testlogic bench
check test testshort testrace bench: gotestdashi
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run "$(TESTS)" $(if $(BENCHES),-bench "$(BENCHES)") -timeout $(TESTTIMEOUT) $(PKG) $(TESTFLAGS)

# Run make testlogic to run all of the logic tests. Specify test files to run
# with make testlogic FILES="foo bar".
testlogic: ## Run SQL Logic Tests.
testlogic: TESTS := $(if $(FILES),TestLogic$$//^$(subst $(space),$$|^,$(FILES))$$,TestLogic)
testlogic: TESTFLAGS := -test.v $(if $(FILES),-show-sql)
testlogic: bin/logictest.test
	cd pkg/sql/logictest && logictest.test -test.run "$(TESTS)" -test.timeout $(TESTTIMEOUT) $(TESTFLAGS)

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
stress stressrace: $(C_LIBS_CCL) $(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET)
	$(GO) list -tags '$(TAGS)' -f '$(XGO) test -v $(GOFLAGS) -tags '\''$(TAGS)'\'' -ldflags '\''$(LINKFLAGS)'\'' -i -c {{.ImportPath}} -o '\''{{.Dir}}'\''/stress.test && (cd '\''{{.Dir}}'\'' && if [ -f stress.test ]; then stress $(STRESSFLAGS) ./stress.test -test.run '\''$(TESTS)'\'' $(if $(BENCHES),-test.bench '\''$(BENCHES)'\'') -test.timeout $(TESTTIMEOUT) $(TESTFLAGS); fi)' $(PKG) | $(SHELL)

.PHONY: upload-coverage
upload-coverage: $(BOOTSTRAP_TARGET)
	$(GO) install $(REPO_ROOT)/vendor/github.com/wadey/gocovmerge
	$(GO) install $(REPO_ROOT)/vendor/github.com/mattn/goveralls
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
	       -not -name 'embedded.go' \
	       -not -name '*_string.go' \
	       -not -name 'sql.go'      \
	       -not -name 'irgen.go'    \
	       -not -name '*.ir.go'     \
	| dupl -files $(DUPLFLAGS)

# All packages need to be installed before we can run (some) of the checks and
# code generators reliably. More precisely, anything that uses x/tools/go/loader
# is fragile (this includes stringer, vet and others). The blocking issue is
# https://github.com/golang/go/issues/14120.

# `go generate` uses stringer and so must depend on gotestdashi per the above
# comment. See https://github.com/golang/go/issues/10249 for details.
.PHONY: generate
generate: ## Regenerate generated code.
generate: gotestdashi
	$(GO) generate $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' $(PKG)

# The style checks depend on `go vet` and so must depend on gotestdashi per the
# above comment. See https://github.com/golang/go/issues/16086 for details.
.PHONY: lint
lint: ## Run all style checkers and linters.
lint: override TAGS += lint
lint: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -run 'TestStyle/$(TESTS)'

.PHONY: lintshort
lintshort: ## Run a fast subset of the style checkers and linters.
lintshort: override TAGS += lint
lintshort: gotestdashi
	$(XGO) test ./build -v $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -short -run 'TestStyle/$(TESTS)'

.PHONY: clean
clean: ## Clean all build artifacts.
clean: clean-c-deps
	$(GO) clean $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -i github.com/cockroachdb/...
	$(FIND_RELEVANT) -type f \( -name 'zcgo_flags*.go' -o -name '*.test' \) -exec rm {} +
	rm -f $(BOOTSTRAP_TARGET) $(ARCHIVE)

.PHONY: protobuf
protobuf: ## Regenerate generated code for protobuf definitions.
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

# pre-push locally runs most of the checks CI will run. Notably, it doesn't run
# the acceptance tests.
.PHONY: pre-push
pre-push: ## Run generate, lint, and test.
pre-push: generate lint test
	$(MAKE) -C $(REPO_ROOT)/pkg/ui lint test
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

# TODO(benesch): Make this recipe use `git ls-files --recurse-submodules`
# instead of scripts/ls-files.sh once Git v2.11 is widely deployed.
.INTERMEDIATE: $(ARCHIVE).tmp
$(ARCHIVE).tmp: ARCHIVE_BASE = cockroach-$(shell cat .buildinfo/tag)
$(ARCHIVE).tmp: .buildinfo/tag .buildinfo/rev .buildinfo/basebranch
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

.buildinfo/basebranch: | .buildinfo
	@git describe --tags --abbrev=0 | tr -d \\n > $@

.buildinfo/rev: | .buildinfo
	@git rev-parse HEAD > $@

ifneq ($(GIT_DIR),)
# If we're in a Git checkout, we update the buildinfo information on every build
# to keep it up-to-date.
.buildinfo/tag: .ALWAYS_REBUILD
.buildinfo/rev: .ALWAYS_REBUILD
.buildinfo/basebranch: .ALWAYS_REBUILD
endif
