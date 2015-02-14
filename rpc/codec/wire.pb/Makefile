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

PROTOC      := $(shell git rev-parse --show-cdup)_vendor/usr/bin/protoc
PROTOS      := $(wildcard *.proto)
PROTO_GO    := $(PROTOS:.proto=.pb.go)
GOGO_PROTOS := ../../../../github.com/gogo/protobuf/gogoproto/gogo.proto

PROTO_PATH := ../../../../../../:../../../../../../github.com/gogo/protobuf/protobuf:../../../../../../github.com/gogo/protobuf/gogoproto

all: $(PROTO_GO)

$(PROTO_GO): $(PROTOS)
	$(PROTOC) --gogo_out=. --proto_path=.:$(PROTO_PATH) $(PROTOS)

clean:
	rm -f $(PROTO_GO)
