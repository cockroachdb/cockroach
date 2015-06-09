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

REPO_ROOT      := $(realpath ..)
ORG_ROOT       := $(REPO_ROOT)/..
GITHUB_ROOT    := $(ORG_ROOT)/..
GOGOPROTO_ROOT := $(GITHUB_ROOT)/gogo/protobuf

ENGINE_ROOT := $(REPO_ROOT)/storage/engine

# Ensure we only have one entry in GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                                   ^  ^  ^~ GOPATH
#                                   |  |~ GOPATH/src

GOPATH_BIN := $(GOPATH)/bin

PROTOC          := $(GOPATH_BIN)/protoc
PROTOC_GEN_GOGO := $(GOPATH_BIN)/protoc-gen-gogo
GOGO_PROTO      := $(GOGOPROTO_ROOT)/gogoproto/gogo.proto
PROTO_PATH      := $(GOGOPROTO_ROOT):$(GOGOPROTO_ROOT)/protobuf
ROACH_PROTOS    := $(sort $(wildcard *.proto))
PREFIXED_PROTOS := $(ROACH_PROTOS:%=cockroach/proto/%)

ROACH_CPP_PROTOS  := $(filter %api.proto %data.proto %internal.proto %config.proto %errors.proto, $(PREFIXED_PROTOS))
ROACH_CPP_HEADERS := $(ROACH_CPP_PROTOS:cockroach/proto/%.proto=$(ENGINE_ROOT)/cockroach/proto/%.pb.h)
ROACH_CPP_SOURCES := $(ROACH_CPP_PROTOS:cockroach/proto/%.proto=$(ENGINE_ROOT)/%.pb.cc)

ROACH_GO_SOURCES  := $(ROACH_PROTOS:%.proto=%.pb.go)

.PHONY:
all: $(ROACH_CPP_HEADERS) $(ROACH_CPP_SOURCES) $(ROACH_GO_SOURCES)

# Protoc is very picky about paths. It doesn't like '..' in any of the input filenames,
# and generally expects paths and package names to match (this is especially important
# when compiling the proto files to python). We must work from a few levels higher in
# the tree so we can use the package name "cockroach.proto". (go is fine with a package
# name like "proto" but that won't work for other languages where package names come
# from a global namespace).

# For c++, it generates a directory structure mirroring the package structure
# (and these directories must be in the include path), but cgo can only compile
# a single directory so we symlink the generated pb.cc files into the storage/engine
# directory.
$(ROACH_CPP_HEADERS) $(ROACH_CPP_SOURCES): $(notdir $(ROACH_CPP_PROTOS)) $(GOGO_PROTO) $(PROTOC) $(PROTOC_GEN_GOGO)
	(cd $(ORG_ROOT) && $(PROTOC) --proto_path=.:$(PROTO_PATH) --cpp_out=$(REPO_ROOT)/storage/engine $(GOGO_PROTO) $(ROACH_CPP_PROTOS))
	(cd $(ENGINE_ROOT) && ln -sf gogoproto/gogo.pb.cc .)
	(cd $(ENGINE_ROOT) && ln -sf cockroach/proto/*.pb.cc .)

$(ROACH_GO_SOURCES): $(ROACH_PROTOS) $(GOGO_PROTO) $(PROTOC) $(PROTOC_GEN_GOGO)
	(cd $(ORG_ROOT) && $(PROTOC) --proto_path=.:$(PROTO_PATH) --plugin=$(PROTOC_GEN_GOGO) --gogo_out=. $(PREFIXED_PROTOS))
	# The gogoprotobuf customtype and unmarshaller extensions do not play nicely with integer
	# types. Manually whack the generate code to make it compile.
	# The below `.bak` is a dirty hack to work around an incompatability between
	# GNU sed and BSD sed. See http://stackoverflow.com/questions/5694228
	sed -i.bak -E 's/(Node|Store)ID \|= \(int32/\1ID |= (\1ID/g' $(shell git grep -l -E '\(gogoproto\.customtype\) = "(Node|Store)ID"' -- *.proto | sed s/\.proto/.pb.go/)
	rm *.pb.go.bak
