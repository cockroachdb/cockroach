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
# Author: Tamir Duberstein (tamird@gmail.com)

REPO_ROOT      := $(realpath ../../..)
ORG_ROOT       := $(REPO_ROOT)/..
GITHUB_ROOT    := $(ORG_ROOT)/..
GOGOPROTO_ROOT := $(GITHUB_ROOT)/gogo/protobuf

# Ensure we only have one entry in GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN := $(GOPATH)/bin

PROTOC          := $(GOPATH_BIN)/protoc
PROTOC_GEN_GOGO := $(GOPATH_BIN)/protoc-gen-gogo
GOGO_PROTO      := $(GOGOPROTO_ROOT)/gogoproto/gogo.proto
PROTO_PATH      := $(GOGOPROTO_ROOT):$(GOGOPROTO_ROOT)/protobuf
RPC_PROTOS      := $(sort $(wildcard *.proto))

RPC_GO_SOURCES  := $(RPC_PROTOS:%.proto=%.pb.go)

.PHONY:
all: $(RPC_GO_SOURCES)

$(RPC_GO_SOURCES): $(RPC_PROTOS) $(GOGO_PROTO) $(PROTOC) $(PROTOC_GEN_GOGO)
	$(PROTOC) --proto_path=.:$(PROTO_PATH) --plugin=$(PROTOC_GEN_GOGO) --gogo_out=. $(RPC_PROTOS)
