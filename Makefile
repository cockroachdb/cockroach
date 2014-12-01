# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
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
GOPATH  := $(CURDIR)/_vendor:$(GOPATH)
# Exposes protoc.
PATH := $(CURDIR)/_vendor/usr/bin:$(PATH)
# Expose protobuf.
export CPLUS_INCLUDE_PATH := $(CURDIR)/_vendor/usr/include:$(CPLUS_INCLUDE_PATH)
export LIBRARY_PATH := $(CURDIR)/_vendor/usr/lib:$(LIBRARY_PATH)

ROACH_PROTO := proto
ROACH_LIB   := roachlib
SQL_PARSER  := sql/parser

PKG        := "./..."
TESTS      := ".*"
TESTFLAGS  := -logtostderr -timeout 10s
RACEFLAGS  := -logtostderr -timeout 1m
BENCHFLAGS := -logtostderr -timeout 5m

OS := $(shell uname -s)

ifeq ($(OS),Darwin)
LDEXTRA += -lc++
endif

ifeq ($(OS),Linux)
LDEXTRA += -lrt
endif

ifeq ($(STATIC),1)
GOFLAGS  += -a -tags netgo -ldflags '-extldflags "-lm -lstdc++ -static"'
endif

all: build test

auxiliary: storage/engine/engine.pc roach_proto roach_lib sqlparser

build: auxiliary
	$(GO) build $(GOFLAGS) -i -o cockroach

storage/engine/engine.pc: storage/engine/engine.pc.in
	sed -e "s,@PWD@,$(CURDIR),g" -e "s,@LDEXTRA@,$(LDEXTRA),g" < $^ > $@

roach_proto:
	make -C $(ROACH_PROTO) static_lib

roach_lib: roach_proto
	make -C $(ROACH_LIB) static_lib

sqlparser:
	make -C $(SQL_PARSER)

goget:
	$(GO) get ./...

test: auxiliary
	$(GO) test $(GOFLAGS) -run $(TESTS) $(PKG) $(TESTFLAGS)

testrace: auxiliary
	$(GO) test $(GOFLAGS) -race -run $(TESTS) $(PKG) $(RACEFLAGS)

bench: auxiliary
	$(GO) test $(GOFLAGS) -benchtime 1s -run '^$$' -bench . $(PKG) $(BENCHFLAGS)

# Build, but do not run the tests. This is used to verify the deployable
# Docker image, which is statically linked and has no build tools in its
# environment. See ./build/deploy for details.
# The test files are moved to the corresponding package. For example,
# PKG=./storage/engine will generate ./storage/engine/engine.test.
#
# TODO(Tobias): This section needs improvement. Some packages don't end up
# statically linked even when STATIC=1 is supplied; libpthread seems to play
# a role. Currently able to work around this by linking these packages
# without CGO in that case.
testbuild: TESTS := $(shell $(GO) list $(PKG))
testbuild: GOFLAGS += -c
testbuild: auxiliary
	for p in $(TESTS); do \
	  NAME=$$(basename "$$p"); \
	  OUT="$$NAME.test"; \
	  DIR=$$($(GO) list -f {{.Dir}} ./...$$NAME); \
	  $(GO) test $(GOFLAGS) "$$p" $(TESTFLAGS) || break; \
	  if [ -f "$$OUT" ]; then \
		if [ ! -z "$(STATIC)" ] && ldd "$$OUT" > /dev/null; then \
		2>&1 echo "$$NAME: rebuilding with CGO_ENABLED=0 to get static binary..."; \
		  CGO_ENABLED=0 $(GO) test $(GOFLAGS) "$$p" $(TESTFLAGS) || break; \
		fi; \
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
	$(GO) clean
	find . -name '*.test' -type f -exec rm {} \;
	rm -f storage/engine/engine.pc
	make -C $(ROACH_PROTO) clean
	make -C $(ROACH_LIB) clean
	make -C $(SQL_PARSER) clean
