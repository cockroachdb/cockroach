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
# Author: Andrew Bonventre (andybons@gmail.com)

PROTO_LIB   := lib/libroachproto.a
PROTOS      := api.proto config.proto data.proto errors.proto gossip.proto heartbeat.proto internal.proto
PROTO_GO    := $(PROTOS:.proto=.pb.go)
GOGO_PROTOS := ../../../../github.com/gogo/protobuf/gogoproto/gogo.proto
SOURCES     := lib/api.pb.cc lib/config.pb.cc lib/data.pb.cc lib/errors.pb.cc lib/gossip.pb.cc lib/heartbeat.pb.cc lib/internal.pb.cc lib/github.com/gogo/protobuf/gogoproto/gogo.pb.cc
HEADERS     := lib/api.pb.h lib/config.pb.h lib/data.pb.h lib/errors.pb.h lib/gossip.pb.h lib/heartbeat.pb.h lib/internal.pb.h lib/github.com/gogo/protobuf/gogoproto/gogo.pb.h
LIBOBJECTS  := $(SOURCES:.cc=.o)

CXXFLAGS += -Ilib

PROTO_PATH := ../../../../:../../../../github.com/gogo/protobuf/protobuf:../../../../github.com/gogo/protobuf/gogoproto
PROTOC_GEN_GOGO :=../../../../../bin/protoc-gen-gogo

all: static_lib

static_lib: $(PROTO_LIB)

$(PROTO_LIB): $(PROTO_GO) $(SOURCES) $(LIBOBJECTS)
	rm -f $@
	ar -rsv $(PROTO_LIB) $(LIBOBJECTS)

$(PROTO_GO): $(PROTOS)
	protoc --plugin=protoc-gen-gogo=$(PROTOC_GEN_GOGO) --gogo_out=. --cpp_out=lib --proto_path=.:$(PROTO_PATH) $(PROTOS)

$(SOURCES): $(GOGO_PROTOS)
	protoc --cpp_out=lib --proto_path=$(PROTO_PATH) $(GOGO_PROTOS)

.cc.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(LIBOBJECTS) $(PROTO_LIB) $(PROTO_GO) $(SOURCES) $(HEADERS)
