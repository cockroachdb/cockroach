# Copyright 2015 The Cockroach Authors.
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

# This file is evaluated in the repo's parent directory. See main.go's
# go:generate invocation.

ORG_ROOT       := .
REPO_ROOT      := $(ORG_ROOT)/cockroach
GITHUB_ROOT    := $(ORG_ROOT)/..
GOGOPROTO_ROOT := $(GITHUB_ROOT)/gogo/protobuf

ENGINE_ROOT := $(REPO_ROOT)/storage/engine

# Ensure we have an unambiguous GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN      := $(GOPATH)/bin
PROTOC          := $(GOPATH_BIN)/protoc
PLUGIN_SUFFIX   := gogo
PROTOC_PLUGIN   := $(GOPATH_BIN)/protoc-gen-$(PLUGIN_SUFFIX)
GOGOPROTO_PROTO := $(GOGOPROTO_ROOT)/gogoproto/gogo.proto
GOGOPROTO_PATH  := $(GOGOPROTO_ROOT):$(GOGOPROTO_ROOT)/protobuf

GO_PROTOS  := $(sort $(shell find $(REPO_ROOT) -not -path '*/.*' -name *.proto -type f))
GO_SOURCES  := $(GO_PROTOS:%.proto=%.pb.go)

CPP_PROTOS := $(filter %api.proto %data.proto %internal.proto %config.proto %errors.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst ./,$(ENGINE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.h)) $(subst $(GOGOPROTO_ROOT),$(ENGINE_ROOT),$(GOGOPROTO_PROTO:%.proto=%.pb.h))
CPP_SOURCES := $(subst ./,$(ENGINE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.cc)) $(subst $(GOGOPROTO_ROOT),$(ENGINE_ROOT),$(GOGOPROTO_PROTO:%.proto=%.pb.cc))

.PHONY: protos
protos: $(GO_SOURCES) $(CPP_HEADERS) $(CPP_SOURCES)

$(GO_SOURCES): $(PROTOC) $(GO_PROTOS) $(GOGOPROTO_PROTO) $(PROTOC_PLUGIN)
	find $(REPO_ROOT) -not -path '*/.*' -name *.pb.go | xargs rm
	for dir in $(sort $(dir $(GO_PROTOS))); do \
	  $(PROTOC) -I.:$(GOGOPROTO_PATH) --plugin=$(PROTOC_PLUGIN) --$(PLUGIN_SUFFIX)_out=$(ORG_ROOT) $$dir/*.proto; \
	done

$(CPP_HEADERS) $(CPP_SOURCES): $(PROTOC) $(CPP_PROTOS) $(GOGOPROTO_PROTO)
	find $(REPO_ROOT) -not -path '*/.*' -name *.pb.h -o -name *.pb.cc | xargs rm
	$(PROTOC) -I.:$(GOGOPROTO_PATH) --cpp_out=$(ENGINE_ROOT) $(CPP_PROTOS)
	$(PROTOC) -I.:$(GOGOPROTO_PATH) --cpp_out=$(ENGINE_ROOT) $(GOGOPROTO_PROTO)
	# For c++, protoc generates a directory structure mirroring the package
	# structure (and these directories must be in the include path), but cgo can
	# only compile a single directory so we symlink the generated pb.cc files
	# into the storage/engine directory.
	(cd $(ENGINE_ROOT) && find . -name *.pb.cc | xargs -I % ln -sf % .)
