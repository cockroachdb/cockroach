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
# Author: Spencer Kimball (spencer.kimball@gmail.com)

# Ensure we only have one entry in GOPATH
GOPATH := $(shell pwd)/../../../../../../..

PROTOC      := $(GOPATH)/bin/protoc
PROTOS      := $(sort $(wildcard *.proto))
PROTO_GO    := $(PROTOS:.proto=.pb.go)

PROTO_PATH := ../..:$(GOPATH)/src/github.com/gogo/protobuf:$(GOPATH)/src/github.com/gogo/protobuf/protobuf
PROTOC_GEN_GOGO := $(GOPATH)/bin/protoc-gen-gogo

all:
	$(PROTOC) --plugin=$(PROTOC_GEN_GOGO) --gogo_out=. --proto_path=.:$(PROTO_PATH) $(PROTOS)
