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
PKG_ROOT       := $(REPO_ROOT)/pkg
GITHUB_ROOT    := $(REPO_ROOT)/vendor/github.com
GOGOPROTO_ROOT := $(GITHUB_ROOT)/gogo/protobuf
PROTOBUF_ROOT  := $(GOGOPROTO_ROOT)/protobuf

NATIVE_ROOT := $(PKG_ROOT)/storage/engine/rocksdb

# Ensure we have an unambiguous GOPATH
GOPATH := $(realpath $(ORG_ROOT)/../../..)
#                                   ^  ^~ GOPATH
#                                   |~ GOPATH/src

GOPATH_BIN      := $(GOPATH)/bin
PROTOC          := $(GOPATH_BIN)/protoc
PLUGIN_SUFFIX   := gogoroach
PROTOC_PLUGIN   := $(GOPATH_BIN)/protoc-gen-$(PLUGIN_SUFFIX)
GOGOPROTO_PROTO := $(GOGOPROTO_ROOT)/gogoproto/gogo.proto

COREOS_PATH := $(GITHUB_ROOT)/coreos
COREOS_RAFT_PROTOS := $(addprefix $(COREOS_PATH)/etcd/raft/, $(sort $(shell git -C $(COREOS_PATH)/etcd/raft ls-files --exclude-standard --cached --others -- '*.proto')))

GRPC_GATEWAY_PACKAGE := github.com/grpc-ecosystem/grpc-gateway
GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := $(GRPC_GATEWAY_PACKAGE)/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := $(GITHUB_ROOT)/../$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes of annotations.proto to the Go package containing the
# generated Go code.
GRPC_GATEWAY_MAPPING := Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api

GW_SERVER_PROTOS := $(PKG_ROOT)/server/serverpb/admin.proto $(PKG_ROOT)/server/serverpb/status.proto
GW_TS_PROTOS := $(PKG_ROOT)/ts/tspb/timeseries.proto

GW_PROTOS  := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)
GW_SOURCES := $(GW_PROTOS:%.proto=%.pb.gw.go)

GO_PROTOS := $(addprefix $(REPO_ROOT)/, $(sort $(shell git -C $(REPO_ROOT) ls-files --exclude-standard --cached --others -- '*.proto')))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

UI_SOURCES := $(PKG_ROOT)/ui/app/js/protos.js $(PKG_ROOT)/ui/generated/protos.json $(PKG_ROOT)/ui/generated/protos.d.ts

CPP_PROTOS := $(filter %/roachpb/metadata.proto %/roachpb/data.proto %/roachpb/internal.proto %/engine/enginepb/mvcc.proto %/engine/enginepb/rocksdb.proto %/hlc/timestamp.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.h))
CPP_SOURCES := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.cc))

.PHONY: protos
protos: $(GO_SOURCES) $(UI_SOURCES) $(CPP_HEADERS) $(CPP_SOURCES) $(ENGINE_CPP_HEADERS) $(ENGINE_CPP_SOURCES) $(GW_SOURCES)

REPO_NAME := cockroachdb
IMPORT_PREFIX := github.com/$(REPO_NAME)/

$(GO_SOURCES): $(PROTOC) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.go' | xargs rm -f)
	for dir in $(sort $(dir $(GO_PROTOS))); do \
	  $(PROTOC) -I.:$(GOGOPROTO_ROOT):$(PROTOBUF_ROOT):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --plugin=$(PROTOC_PLUGIN) --$(PLUGIN_SUFFIX)_out=$(GRPC_GATEWAY_MAPPING),plugins=grpc,import_prefix=$(IMPORT_PREFIX):$(ORG_ROOT) $$dir/*.proto; \
	done
	sed -i~ -E '/gogoproto/d' $(GO_SOURCES)
	sed -i~ -E 's!import (fmt|math) "$(IMPORT_PREFIX)(fmt|math)"! !g' $(GO_SOURCES)
	sed -i~ -E 's!$(IMPORT_PREFIX)(errors|fmt|io|github\.com|golang\.org|google\.golang\.org)!\1!g' $(GO_SOURCES)
	sed -i~ -E 's!$(REPO_NAME)/(etcd)!coreos/\1!g' $(GO_SOURCES)
	gofmt -s -w $(GO_SOURCES)

$(GW_SOURCES) : $(GW_SERVER_PROTOS) $(GW_TS_PROTOS) $(GO_PROTOS) $(GOGOPROTO_PROTO) $(PROTOC)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.gw.go' | xargs rm -f)
	$(PROTOC) -I.:$(GOGOPROTO_ROOT):$(PROTOBUF_ROOT):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true:. $(GW_SERVER_PROTOS)
	$(PROTOC) -I.:$(GOGOPROTO_ROOT):$(PROTOBUF_ROOT):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true:. $(GW_TS_PROTOS)

$(REPO_ROOT)/build/yarn.installed: $(REPO_ROOT)/build/package.json
	cd $(REPO_ROOT)/build && yarn install
	touch $@

PBJS_ARGS = --path $(ORG_ROOT) --path $(GOGOPROTO_ROOT) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(GW_PROTOS)

$(PKG_ROOT)/ui/app/js/protos.js: $(REPO_ROOT)/build/yarn.installed $(GO_PROTOS) $(COREOS_RAFT_PROTOS)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(REPO_ROOT)/build/node_modules/.bin/pbjs -t commonjs $(PBJS_ARGS) >> $@

$(PKG_ROOT)/ui/generated/protos.json: $(REPO_ROOT)/build/yarn.installed $(GO_PROTOS) $(COREOS_RAFT_PROTOS)
	$(REPO_ROOT)/build/node_modules/.bin/pbjs $(PBJS_ARGS) > $@

$(PKG_ROOT)/ui/generated/protos.d.ts: $(PKG_ROOT)/ui/generated/protos.json
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(REPO_ROOT)/build/node_modules/.bin/proto2ts --file $(PKG_ROOT)/ui/generated/protos.json >> $@
	sed -i~ -E '/delete : string/d' $@ # This line produces a duplicate identifier error. Why?

$(CPP_HEADERS) $(CPP_SOURCES): $(PROTOC) $(CPP_PROTOS)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.h' '*.pb.cc' | xargs rm -f)
	$(PROTOC) -I.:$(GOGOPROTO_ROOT):$(PROTOBUF_ROOT) --cpp_out=lite:$(NATIVE_ROOT) $(CPP_PROTOS)
	sed -i~ -E '/gogoproto/d' $(CPP_HEADERS) $(CPP_SOURCES)
	@# For c++, protoc generates a directory structure mirroring the package
	@# structure (and these directories must be in the include path), but cgo can
	@# only compile a single directory so we symlink the generated pb.cc files
	@# into the storage/engine directory.
	@# We use `find` and not `git ls-files` here because `git ls-files` will
	@# include deleted files (i.e. these very symlinks) in its output, resulting
	@# in recursive symlinks, which is Bad™.
	(cd $(NATIVE_ROOT) && find . -name *.pb.cc | xargs -I % ln -sf % .)
