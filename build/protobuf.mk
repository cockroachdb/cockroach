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
# permissions and limitations under the License.
#
# Author: Tamir Duberstein (tamird@gmail.com)

# This file is evaluated in the repo's parent directory. See main.go's
# go:generate invocation.

ORG_ROOT       := .
REPO_ROOT      := $(ORG_ROOT)/cockroach
GITHUB_ROOT    := $(ORG_ROOT)/..
GOGOPROTO_ROOT := $(GITHUB_ROOT)/gogo/protobuf

NATIVE_ROOT := $(REPO_ROOT)/storage/engine/rocksdb

# Ensure we have an unambiguous GOPATH
GOPATH := $(realpath $(GITHUB_ROOT)/../..)
#                                   ^  ^~ GOPATH
#                                   |~ GOPATH/src

GOPATH_BIN      := $(GOPATH)/bin
PROTOC          := $(GOPATH_BIN)/protoc
PLUGIN_SUFFIX   := gogoroach
PROTOC_PLUGIN   := $(GOPATH_BIN)/protoc-gen-$(PLUGIN_SUFFIX)
GOGOPROTO_PROTO := $(GOGOPROTO_ROOT)/gogoproto/gogo.proto
GOGOPROTO_PATH  := $(GOGOPROTO_ROOT):$(GOGOPROTO_ROOT)/protobuf
CPROTOBUF_PATH  := $(ORG_ROOT)/c-protobuf/internal/src

COREOS_PATH := $(GITHUB_ROOT)/coreos

GRPC_GATEWAY_PACKAGE := github.com/gengo/grpc-gateway
GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := $(GRPC_GATEWAY_PACKAGE)/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := $(GITHUB_ROOT)/../$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes of annotations.proto to the Go package containing the
# generated Go code.
GRPC_GATEWAY_MAPPING := Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api

ADMIN_PROTO := $(REPO_ROOT)/server/admin.proto
ADMIN_GW_GO := $(ADMIN_PROTO:%.proto=%.pb.gw.go)

GO_PROTOS := $(addprefix $(REPO_ROOT)/, $(sort $(shell cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.proto')))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

CPP_PROTOS := $(filter %/metadata.proto %/data.proto %/internal.proto %/mvcc.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.h)) $(subst $(GOGOPROTO_ROOT),$(NATIVE_ROOT),$(GOGOPROTO_PROTO:%.proto=%.pb.h))
CPP_SOURCES := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.cc)) $(subst $(GOGOPROTO_ROOT),$(NATIVE_ROOT),$(GOGOPROTO_PROTO:%.proto=%.pb.cc))

ENGINE_CPP_PROTOS := $(filter $(NATIVE_ROOT)%,$(GO_PROTOS))
ENGINE_CPP_HEADERS := $(ENGINE_CPP_PROTOS:%.proto=%.pb.h)
ENGINE_CPP_SOURCES := $(ENGINE_CPP_PROTOS:%.proto=%.pb.cc)

.PHONY: protos
protos: $(GO_SOURCES) $(CPP_HEADERS) $(CPP_SOURCES) $(ENGINE_CPP_HEADERS) $(ENGINE_CPP_SOURCES) $(ADMIN_GW_GO)

REPO_NAME := cockroachdb
IMPORT_PREFIX := github.com/$(REPO_NAME)/

$(GO_SOURCES): $(PROTOC) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.go' | xargs rm -f)
	for dir in $(sort $(dir $(GO_PROTOS))); do \
	  $(PROTOC) -I.:$(GOGOPROTO_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH):$(CPROTOBUF_PATH) --plugin=$(PROTOC_PLUGIN) --$(PLUGIN_SUFFIX)_out=$(GRPC_GATEWAY_MAPPING),plugins=grpc,import_prefix=$(IMPORT_PREFIX):$(ORG_ROOT) $$dir/*.proto; \
	  sed -i~ -E 's!import (fmt|math) "$(IMPORT_PREFIX)(fmt|math)"! !g' $$dir/*.pb.go; \
	  sed -i~ -E 's!$(IMPORT_PREFIX)(errors|fmt|io|github\.com|golang\.org|google\.golang\.org)!\1!g' $$dir/*.pb.go; \
	  sed -i~ -E 's!$(REPO_NAME)/(etcd)!coreos/\1!g' $$dir/*.pb.go; \
	  gofmt -s -w $$dir/*.pb.go; \
	done

$(ADMIN_GW_GO) : $(ADMIN_PROTO) $(GO_PROTOS) $(GOGOPROTO_PROTO) $(PROTOC)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.gw.go' | xargs rm -f)
	$(PROTOC) -I.:$(GOGOPROTO_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH):$(CPROTOBUF_PATH) --grpc-gateway_out=logtostderr=true:. $(ADMIN_PROTO)
	sed -i~ -E 's!golang/protobuf/proto!gogo/protobuf/proto!' $(ADMIN_GW_GO)

$(CPP_HEADERS) $(CPP_SOURCES): $(PROTOC) $(CPP_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.h' '*.pb.cc' | xargs rm -f)
	$(PROTOC) -I.:$(GOGOPROTO_PATH) --cpp_out=$(NATIVE_ROOT) $(CPP_PROTOS)
	$(PROTOC) -I.:$(GOGOPROTO_PATH) --cpp_out=$(NATIVE_ROOT) $(GOGOPROTO_PROTO)
	@# For c++, protoc generates a directory structure mirroring the package
	@# structure (and these directories must be in the include path), but cgo can
	@# only compile a single directory so we symlink the generated pb.cc files
	@# into the storage/engine directory.
	@# We use `find` and not `git ls-files` here because `git ls-files` will
	@# include deleted files (i.e. these very symlinks) in its output, resulting
	@# in recursive symlinks, which is Bad™.
	(cd $(NATIVE_ROOT) && find . -name *.pb.cc | xargs -I % ln -sf % .)

$(ENGINE_CPP_HEADERS) $(ENGINE_CPP_SOURCES): $(PROTOC) $(ENGINE_CPP_PROTOS)
	$(PROTOC) -I.:$(GOGOPROTO_PATH) --cpp_out=$(ORG_ROOT) $(ENGINE_CPP_PROTOS)
