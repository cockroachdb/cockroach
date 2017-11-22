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

## Space delimited list of logic test files to run, for make testlogic.
FILES :=

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

REPO_ROOT := .
MAKEFLAGS += $(shell $(REPO_ROOT)/build/jflag.sh)

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
		"make testlogic FILES=prepare" "Run the logic test with filename prepare."

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

.DEFAULT_GOAL := all
all: $(COCKROACH)

buildoss: BUILDTARGET = ./pkg/cmd/cockroach-oss
buildoss: $(C_LIBS_OSS) $(UI_ROOT)/distoss/bindata.go

buildshort: BUILDTARGET = ./pkg/cmd/cockroach-short
buildshort: $(C_LIBS_CCL)

$(COCKROACH) build go-install gotestdashi generate lint lintshort: $(C_LIBS_CCL)
$(COCKROACH) build go-install generate: $(UI_ROOT)/distccl/bindata.go

$(COCKROACH) build buildoss buildshort: BUILDMODE = build -i -o $(COCKROACH)

BUILDINFO = .buildinfo/tag .buildinfo/rev .buildinfo/basebranch

# The build.utcTime format must remain in sync with TimeFormat in pkg/build/info.go.
$(COCKROACH) build buildoss buildshort go-install gotestdashi generate lint lintshort: \
	$(CGO_FLAGS_FILES) $(BOOTSTRAP_TARGET) $(SQLPARSER_TARGETS) $(BUILDINFO)
$(COCKROACH) build buildoss buildshort go-install gotestdashi generate lint lintshort: override LINKFLAGS += \
	-X "github.com/cockroachdb/cockroach/pkg/build.tag=$(shell cat .buildinfo/tag)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.utcTime=$(shell date -u '+%Y/%m/%d %H:%M:%S')" \
	-X "github.com/cockroachdb/cockroach/pkg/build.rev=$(shell cat .buildinfo/rev)" \
	-X "github.com/cockroachdb/cockroach/pkg/build.baseBranch=$(shell cat .buildinfo/basebranch)"

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
FIND_RELEVANT := find pkg -name node_modules -prune -o

bin/logictest.test: PKG := ./pkg/sql/logictest
bin/logictest.test: main.go $(shell $(FIND_RELEVANT) ! -name 'zcgo_flags.go' -name '*.go')
	$(MAKE) gotestdashi GOFLAGS='$(GOFLAGS)' TAGS='$(TAGS)' LINKFLAGS='$(LINKFLAGS)' PKG='$(PKG)'
	$(XGO) test $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LINKFLAGS)' -c -o bin/logictest.test $(PKG)

bench: ## Run benchmarks.
bench: TESTS := -
bench: BENCHES := .
bench: TESTTIMEOUT := $(BENCHTIMEOUT)

.PHONY: check test testshort testrace testlogic bench
test: ## Run tests.
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
stress stressrace: gotestdashi
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
protobuf: $(GO_PROTOS_TARGET) $(GW_PROTOS_TARGET) $(CPP_PROTOS_TARGET)
protobuf: ## Regenerate generated code for protobuf definitions.

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
$(ARCHIVE).tmp: $(UI_ROOT)/distoss/bindata.go $(UI_ROOT)/distccl/bindata.go $(SQLPARSER_TARGETS) $(BUILDINFO)
	scripts/ls-files.sh | $(TAR) -cf $@ -T - $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, $^
	$(TAR) -rf $@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, pkg/ui/distccl/bindata.go pkg/ui/distoss/bindata.go
	$(TAR) -rf $@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, pkg/sql/lex/keywords.go pkg/sql/lex/reserved_keywords.go pkg/sql/lex/tokens.go
	$(TAR) -rf $@ $(TAR_XFORM_FLAG),^,$(ARCHIVE_BASE)/src/github.com/cockroachdb/cockroach/, pkg/sql/parser/sql.go pkg/sql/parser/help_messages.go pkg/sql/parser/helpmap_test.go
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

CPP_PROTO_ROOT := $(LIBROACH_SRC_DIR)/protos

GOGO_PROTOBUF_PATH := $(REPO_ROOT)/vendor/github.com/gogo/protobuf
PROTOBUF_PATH  := $(GOGO_PROTOBUF_PATH)/protobuf

PROTOC_PLUGIN   := $(LOCAL_BIN)/protoc-gen-gogoroach
GOGOPROTO_PROTO := $(GOGO_PROTOBUF_PATH)/gogoproto/gogo.proto

COREOS_PATH := $(REPO_ROOT)/vendor/github.com/coreos
COREOS_RAFT_PROTOS := $(addprefix $(COREOS_PATH)/etcd/raft/, $(sort $(shell git -C $(COREOS_PATH)/etcd/raft ls-files --exclude-standard --cached --others -- '*.proto')))

GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := $(REPO_ROOT)/vendor/$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes to the Go package containing the generated Go code.
PROTO_MAPPINGS :=
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api,
PROTO_MAPPINGS := $(PROTO_MAPPINGS)Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,

GW_SERVER_PROTOS := $(PKG_ROOT)/server/serverpb/admin.proto $(PKG_ROOT)/server/serverpb/status.proto $(PKG_ROOT)/server/serverpb/authentication.proto
GW_TS_PROTOS := $(PKG_ROOT)/ts/tspb/timeseries.proto

GW_PROTOS  := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)
GW_SOURCES := $(GW_PROTOS:%.proto=%.pb.gw.go)

GO_PROTOS := $(addprefix $(REPO_ROOT)/, $(sort $(shell git -C $(REPO_ROOT) ls-files --exclude-standard --cached --others -- '*.proto')))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

PBJS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbjs
PBTS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbts

UI_JS := $(UI_ROOT)/src/js/protos.js
UI_TS := $(UI_ROOT)/src/js/protos.d.ts
UI_PROTOS := $(UI_JS) $(UI_TS)

CPP_PROTOS := $(filter %/roachpb/metadata.proto %/roachpb/data.proto %/roachpb/internal.proto %/engine/enginepb/mvcc.proto %/engine/enginepb/mvcc3.proto %/engine/enginepb/rocksdb.proto %/hlc/legacy_timestamp.proto %/hlc/timestamp.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst $(PKG_ROOT),$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.h))
CPP_SOURCES := $(subst $(PKG_ROOT),$(CPP_PROTO_ROOT),$(CPP_PROTOS:%.proto=%.pb.cc))

UI_PROTOS := $(UI_JS) $(UI_TS)

$(GO_PROTOS_TARGET): $(PROTOC) $(PROTOC_PLUGIN) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.go' | xargs rm -f)
	for dir in $(sort $(dir $(GO_PROTOS))); do \
	  $(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --plugin=$(PROTOC_PLUGIN) --gogoroach_out=$(PROTO_MAPPINGS),plugins=grpc,import_prefix=github.com/cockroachdb/cockroach/pkg/:$(PKG_ROOT) $$dir/*.proto; \
	done
	$(SED_INPLACE) '/import _/d' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!import (fmt|math) "github.com/cockroachdb/cockroach/pkg/(fmt|math)"! !g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!cockroachdb/cockroach/pkg/(etcd)!coreos/\1!g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!github.com/cockroachdb/cockroach/pkg/(bytes|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)!\1!g' $(GO_SOURCES)
	gofmt -s -w $(GO_SOURCES)
	touch $@

$(GW_PROTOS_TARGET): $(PROTOC) $(GW_SERVER_PROTOS) $(GW_TS_PROTOS) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.gw.go' | xargs rm -f)
	$(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:$(PKG_ROOT) $(GW_SERVER_PROTOS)
	$(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:$(PKG_ROOT) $(GW_TS_PROTOS)
	touch $@

$(CPP_PROTOS_TARGET): $(PROTOC) $(CPP_PROTOS)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.h' '*.pb.cc' | xargs rm -f)
	mkdir -p $(CPP_PROTO_ROOT)
	$(PROTOC) -I$(PKG_ROOT):$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(CPP_PROTO_ROOT) $(CPP_PROTOS)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS) $(CPP_SOURCES)
	touch $@

$(UI_JS): $(GO_PROTOS) $(COREOS_RAFT_PROTOS) $(YARN_INSTALLED_TARGET)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBJS) -t static-module -w es6 --strict-long --keep-case --path $(PKG_ROOT) --path $(GOGO_PROTOBUF_PATH) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(GW_PROTOS) >> $@

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
$(UI_ROOT)/dist/%.dll.js $(UI_ROOT)/%-manifest.json: $(UI_ROOT)/webpack.%.js $(YARN_INSTALLED_TARGET) $(UI_PROTOS)
	$(NODE_RUN) -C $(UI_ROOT) $(WEBPACK) -p --config webpack.$*.js

.PHONY: ui-test
ui-test: $(UI_DLLS) $(UI_MANIFESTS)
	$(NODE_RUN) -C $(UI_ROOT) $(KARMA) start

.PHONY: ui-test-watch
ui-test-watch: $(UI_DLLS) $(UI_MANIFESTS)
	$(NODE_RUN) -C $(UI_ROOT) $(KARMA) start --no-single-run --auto-watch

$(UI_ROOT)/dist%/bindata.go: $(UI_ROOT)/webpack.%.js $(UI_DLLS) $(UI_MANIFESTS) $(shell find $(UI_ROOT)/src $(UI_ROOT)/styl)
	@# TODO(benesch): remove references to embedded.go once sufficient time has passed.
	rm -f $(UI_ROOT)/embedded.go
	find $(UI_ROOT)/dist$* -mindepth 1 -not -name dist$*.go -delete
	set -e; for dll in $(notdir $(UI_DLLS)); do ln -s ../dist/$$dll $(UI_ROOT)/dist$*/$$dll; done
	$(NODE_RUN) -C $(UI_ROOT) $(WEBPACK) --config webpack.$*.js
	go-bindata -nometadata -pkg dist$* -o $@ -prefix $(UI_ROOT)/dist$* $(UI_ROOT)/dist$*/...
	$(SED_INPLACE) -f $(UI_ROOT)/process-bindata.sed $@
	gofmt -s -w $@

$(UI_ROOT)/yarn.opt.installed:
	$(NODE_RUN) -C $(UI_ROOT)/opt yarn install
	touch $@

.PHONY: ui-watch
ui-watch: export TARGET := http://localhost:8080
ui-watch: PORT := 3000
ui-watch: $(UI_DLLS) $(UI_ROOT)/yarn.opt.installed
	cd $(UI_ROOT) && $(WEBPACK_DASHBOARD) -- $(WEBPACK_DEV_SERVER) --config webpack.ccl.js --port $(PORT)

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
maintainer-clean: clean
	find $(UI_ROOT)/dist* -mindepth 1 -not -name dist*.go -delete
	rm -rf $(UI_ROOT)/node_modules $(UI_DLLS) $(YARN_INSTALLED_TARGET)
	rm -f $(SQLPARSER_TARGETS) $(UI_PROTOS)

.PHONY: unsafe-clean
unsafe-clean: ## Like maintainer-clean, but also remove ALL untracked/ignored files.
unsafe-clean: maintainer-clean unsafe-clean-c-deps
	git clean -dxf
