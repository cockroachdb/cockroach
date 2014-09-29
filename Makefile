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

DEPLOY      := $(CURDIR)/deploy
GOPATH      := $(CURDIR)/_vendor:$(GOPATH)
ROCKSDB     := $(CURDIR)/_vendor/rocksdb
ROACH_PROTO := $(CURDIR)/proto
ROACH_LIB   := $(CURDIR)/roachlib
SQL_PARSER  := $(CURDIR)/sql/parser

CFLAGS   := "-I$(ROCKSDB)/include -I$(ROACH_PROTO)/lib -I$(ROACH_LIB) $(CFLAGS)"
CXXFLAGS := "-I$(ROCKSDB)/include -I$(ROACH_PROTO)/lib -I$(ROACH_LIB) $(CXXFLAGS)"
LDFLAGS  := "-L/usr/local/lib -L$(ROCKSDB) -L$(ROACH_PROTO)/lib -L$(ROACH_LIB) $(LDFLAGS)"

FLAGS := LDFLAGS=$(LDFLAGS) \
         CFLAGS=$(CFLAGS) \
         CXXFLAGS=$(CXXFLAGS)

CGO_FLAGS := CGO_LDFLAGS=$(LDFLAGS) \
             CGO_CFLAGS=$(CFLAGS) \
             CGO_CXXFLAGS=$(CXXFLAGS)

PKG       := "./..."
TESTS     := ".*"
TESTFLAGS := -logtostderr -timeout 10s

all: build test

auxiliary: rocksdb roach_proto roach_lib sqlparser

build: auxiliary
	$(CGO_FLAGS) $(GO) build -o cockroach

rocksdb:
	# There is a hiccup in RocksDB's Makefile which causes build_version.cc to
	# be "newer" than build_version.o for each run of `make static_lib` even
	# though before invoking make, this is not the case. This is a quick fix
	# for this, simply telling make that build_version.cc hasn't changed.
	cd $(ROCKSDB); make -o util/build_version.cc static_lib

roach_proto:
	cd $(ROACH_PROTO); $(FLAGS) make static_lib

roach_lib: roach_proto
	cd $(ROACH_LIB); $(FLAGS) make static_lib

sqlparser:
	cd $(SQL_PARSER); $(FLAGS) make

goget:
	$(CGO_FLAGS) $(GO) get ./...

test: auxiliary
	$(CGO_FLAGS) $(GO) test -run $(TESTS) $(PKG) $(TESTFLAGS)

testrace: auxiliary
	$(CGO_FLAGS) $(GO) test -race -run $(TESTS) $(PKG) $(TESTFLAGS)

coverage: build
	$(CGO_FLAGS) $(GO) test -cover -run $(TESTS) $(PKG) $(TESTFLAGS)

acceptance:
	cd $(DEPLOY); ./build-docker.sh && ./local-cluster.sh start && ./local-cluster.sh stop

clean:
	$(GO) clean
	cd $(ROCKSDB); make clean
	cd $(ROACH_PROTO); make clean
	cd $(ROACH_LIB); make clean
	cd $(SQL_PARSER); make clean
