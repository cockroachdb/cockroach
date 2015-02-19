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
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.
#
# Author: Andrew Bonventre (andybons@gmail.com)
# Author: Shawn Morel (shawnmorel@gmail.com)
# Author: Spencer Kimball (spencer.kimball@gmail.com)

# Cockroach build rules.
GO ?= go
# Allow setting of go build flags from the command line (see
# .travis.yml).
GOFLAGS := 
# Set to 1 to use static linking for all builds (including tests).
STATIC := $(STATIC)

RUN  := run
GOPATH  := $(CURDIR)/../../../..:$(CURDIR)/_vendor

# TODO(pmattis): Figure out where to set different CGO_* variables when
# building "release" binaries.
export CGO_CPPFLAGS :=-g -I $(CURDIR)/_vendor/usr/include -I $(CURDIR)/_vendor/rocksdb/include
export CGO_LDFLAGS :=-g -L $(CURDIR)/_vendor/usr/lib -L $(CURDIR)/_vendor/rocksdb

PKG        := "./..."
TESTS      := ".*"
TESTFLAGS  := -logtostderr -timeout 10s
RACEFLAGS  := -logtostderr -timeout 1m
BENCHFLAGS := -logtostderr -timeout 5m

ifeq ($(STATIC),1)
GOFLAGS  += -a -tags netgo -ldflags '-extldflags "-lm -lstdc++ -static"'
endif

all: build test

# "go build -i" explicitly does not rebuild dependent packages that
# have a different root directory than the package being built, hence
# the need for a separate build invocation for etcd/raft.
build:
	$(GO) build $(GOFLAGS) -i github.com/coreos/etcd/raft
	$(GO) build $(GOFLAGS) -i -o cockroach

test:
	$(GO) test $(GOFLAGS) -run $(TESTS) $(PKG) $(TESTFLAGS)

testrace:
	$(GO) test $(GOFLAGS) -race -run $(TESTS) $(PKG) $(RACEFLAGS)

bench:
	$(GO) test $(GOFLAGS) -run $(TESTS) -bench $(TESTS) $(PKG) $(BENCHFLAGS)

# Build, but do not run the tests. This is used to verify the deployable
# Docker image which comes without the build environment. See ./build/deploy
# for details.
# The test files are moved to the corresponding package. For example,
# PKG=./storage/engine will generate ./storage/engine/engine.test.
testbuild: TESTS := $(shell $(GO) list $(PKG))
testbuild: GOFLAGS += -c
testbuild:
	for p in $(TESTS); do \
	  NAME=$$(basename "$$p"); \
	  OUT="$$NAME.test"; \
	  DIR=$$($(GO) list -f {{.Dir}} ./...$$NAME); \
	  $(GO) test $(GOFLAGS) "$$p" $(TESTFLAGS) || break; \
	  if [ -f "$$OUT" ]; then \
		mv "$$OUT" "$$DIR" || break; \
	  fi \
	done


coverage: build
	$(GO) test $(GOFLAGS) -cover -run $(TESTS) $(PKG) $(TESTFLAGS)

acceptance:
# The first `stop` stops and cleans up any containers from previous runs.
	(cd $(RUN); \
	  ../build/build-docker-dev.sh && \
	  ./local-cluster.sh stop && \
	  ./local-cluster.sh start && \
	  ./local-cluster.sh stop)

clean:
	$(GO) clean -i -r ./...
	find . -name '*.test' -type f -exec rm -f {} \;
	rm -f storage/engine/engine.pc

# The gopath target outputs the GOPATH that should be used for building this
# package. It is used by the emacs go-projectile package for automatic
# configuration.
gopath:
	@echo -n $(GOPATH)
