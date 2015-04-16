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

PWD=$(shell pwd)

PROTOC      := $(PWD)/../../../../../bin/protoc
PROTOS      := $(sort $(wildcard *.proto))
PROTO_GO    := $(PROTOS:.proto=.pb.go)
GOGO_PROTOS := $(PWD)/../../../../github.com/gogo/protobuf/gogoproto/gogo.proto
SOURCES     := $(PROTOS:.proto=.pb.cc) github.com/gogo/protobuf/gogoproto/gogo.pb.cc
HEADERS     := $(PROTOS:.proto=.pb.h) github.com/gogo/protobuf/gogoproto/gogo.pb.h

PROTO_PATH := $(PWD)/../..:$(PWD)/../../../../github.com/gogo/protobuf:$(PWD)/../../../../github.com/gogo/protobuf/protobuf
PROTOC_GEN_GOGO := $(PWD)/../../../../../bin/protoc-gen-gogo

# Protoc is very picky about paths. It doesn't like '..' in any of the input filenames,
# and generally expects paths and package names to match (this is especially important
# when compiling the proto files to python). We must work from a few levels higher in
# the tree so we can use the package name "cockroach.proto". (go is fine with a package
# name like "proto" but that won't work for other languages where package names come
# from a global namespace).
#
# For c++, it generates a directory structure mirroring the package structure
# (and these directories must be in the include path), but cgo can only compile
# a single directory so we symlink the generated pb.cc files into the storage/engine
# directory.
#
# The gogoprotobuf customtype and unmarshaller extensions do not play nicely with integer
# types. Manually wack the generate code to make it compile.
all:
	(cd ../.. && $(PROTOC) --plugin=protoc-gen-gogo=$(PROTOC_GEN_GOGO) --gogo_out=. --cpp_out=cockroach/storage/engine --proto_path=.:$(PROTO_PATH) $(PROTOS:%=cockroach/proto/%))
	(cd ../.. && $(PROTOC) --cpp_out=cockroach/storage/engine --proto_path=$(PROTO_PATH) $(GOGO_PROTOS))
	(cd ../storage/engine && ln -sf gogoproto/gogo.pb.cc .)
	(cd ../storage/engine && ln -sf cockroach/proto/*.pb.cc .)
	sed -E 's/(Node|Store)ID \|= \(int32/\1ID |= (\1ID/g' < config.pb.go > config.pb.go.new && \
	  mv config.pb.go.new config.pb.go
	sed -E 's/(Node|Store)ID \|= \(int32/\1ID |= (\1ID/g' < data.pb.go > data.pb.go.new && \
	  mv data.pb.go.new data.pb.go
	sed -E 's/(Node|Store)ID \|= \(int32/\1ID |= (\1ID/g' < status.pb.go > status.pb.go.new && \
	  mv status.pb.go.new status.pb.go
