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

DEPLOY  := deploy
GOPATH  := $(CURDIR)/_vendor:$(GOPATH)

ROACH_PROTO := proto
ROACH_LIB   := roachlib
SQL_PARSER  := sql/parser

PKG       := "./..."
TESTS     := ".*"
TESTFLAGS := -logtostderr -timeout 10s

OS := $(shell uname -s)

ifeq ($(OS),Darwin)
LDEXTRA += -lc++
endif

ifeq ($(OS),Linux)
LDEXTRA += -lrt
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
	$(GO) test -run $(TESTS) $(PKG) $(TESTFLAGS)

testrace: auxiliary
	$(GO) test -race -run $(TESTS) $(PKG) $(TESTFLAGS)

coverage: build
	$(GO) test -cover -run $(TESTS) $(PKG) $(TESTFLAGS)

acceptance:
	(cd $(DEPLOY); \
	  ./build-docker.sh && \
	  ./local-cluster.sh start && \
	  ./local-cluster.sh stop)

clean:
	$(GO) clean
	rm -f storage/engine/engine.pc
	make -C $(ROACH_PROTO) clean
	make -C $(ROACH_LIB) clean
	make -C $(SQL_PARSER) clean
