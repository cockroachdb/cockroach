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

# This file is evaluated in the repo's parent directory. See main.go's
# go:generate invocation.
REPO_ROOT := ./cockroach
include $(REPO_ROOT)/build/common.mk

NATIVE_ROOT := $(LIBROACH_SRC_DIR)/protos
GITHUB_ROOT := $(REPO_ROOT)/vendor/github.com

GOGO_PROTOBUF_PACKAGE := github.com/gogo/protobuf
GOGO_PROTOBUF_TYPES_PACKAGE := $(GOGO_PROTOBUF_PACKAGE)/types
GOGO_PROTOBUF_PATH := $(GITHUB_ROOT)/../$(GOGO_PROTOBUF_PACKAGE)
PROTOBUF_PATH  := $(GOGO_PROTOBUF_PATH)/protobuf

PLUGIN_SUFFIX   := gogoroach
PROTOC_PLUGIN   := $(LOCAL_BIN)/protoc-gen-$(PLUGIN_SUFFIX)
GOGOPROTO_PROTO := $(GOGO_PROTOBUF_PATH)/gogoproto/gogo.proto

COREOS_PATH := $(GITHUB_ROOT)/coreos
COREOS_RAFT_PROTOS := $(addprefix $(COREOS_PATH)/etcd/raft/, $(sort $(shell git -C $(COREOS_PATH)/etcd/raft ls-files --exclude-standard --cached --others -- '*.proto')))

GRPC_GATEWAY_PACKAGE := github.com/grpc-ecosystem/grpc-gateway
GRPC_GATEWAY_PLUGIN  := $(LOCAL_BIN)/protoc-gen-grpc-gateway
GRPC_GATEWAY_GOOGLEAPIS_PACKAGE := $(GRPC_GATEWAY_PACKAGE)/third_party/googleapis
GRPC_GATEWAY_GOOGLEAPIS_PATH := $(GITHUB_ROOT)/../$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)

# Map protobuf includes to the Go package containing the generated Go code.
MAPPINGS :=
MAPPINGS := $(MAPPINGS)Mgoogle/api/annotations.proto=$(GRPC_GATEWAY_GOOGLEAPIS_PACKAGE)/google/api,
MAPPINGS := $(MAPPINGS)Mgoogle/protobuf/timestamp.proto=$(GOGO_PROTOBUF_PACKAGE)/types,

GW_SERVER_PROTOS := $(PKG_ROOT)/server/serverpb/admin.proto $(PKG_ROOT)/server/serverpb/status.proto $(PKG_ROOT)/server/serverpb/authentication.proto
GW_TS_PROTOS := $(PKG_ROOT)/ts/tspb/timeseries.proto

GW_PROTOS  := $(GW_SERVER_PROTOS) $(GW_TS_PROTOS)
GW_SOURCES := $(GW_PROTOS:%.proto=%.pb.gw.go)

GO_PROTOS := $(addprefix $(REPO_ROOT)/, $(sort $(shell git -C $(REPO_ROOT) ls-files --exclude-standard --cached --others -- '*.proto')))
GO_SOURCES := $(GO_PROTOS:%.proto=%.pb.go)

PBJS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbjs
PBTS := $(NODE_RUN) $(UI_ROOT)/node_modules/.bin/pbts

UI_JS := $(UI_ROOT)/src/js/protos.js
UI_TS := $(UI_ROOT)/src/js/protos.d.ts
UI_SOURCES := $(UI_JS) $(UI_TS)

CPP_PROTOS := $(filter %/roachpb/metadata.proto %/roachpb/data.proto %/roachpb/internal.proto %/engine/enginepb/mvcc.proto %/engine/enginepb/mvcc3.proto %/engine/enginepb/rocksdb.proto %/hlc/legacy_timestamp.proto %/hlc/timestamp.proto %/unresolved_addr.proto,$(GO_PROTOS))
CPP_HEADERS := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.h))
CPP_SOURCES := $(subst ./,$(NATIVE_ROOT)/,$(CPP_PROTOS:%.proto=%.pb.cc))

ORG_NAME := cockroachdb
IMPORT_PREFIX := github.com/$(ORG_NAME)/

GO_SOURCES_TARGET := $(LOCAL_BIN)/.go_protobuf_sources
$(GO_SOURCES_TARGET): $(PROTOC) $(PROTOC_PLUGIN) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.go' | xargs rm -f)
	for dir in $(sort $(dir $(GO_PROTOS))); do \
	  $(PROTOC) -I.:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --plugin=$(PROTOC_PLUGIN) --$(PLUGIN_SUFFIX)_out=$(MAPPINGS),plugins=grpc,import_prefix=$(IMPORT_PREFIX):$(ORG_ROOT) $$dir/*.proto; \
	done
	$(SED_INPLACE) '/import _/d' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!import (fmt|math) "$(IMPORT_PREFIX)(fmt|math)"! !g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!$(ORG_NAME)/(etcd)!coreos/\1!g' $(GO_SOURCES)
	$(SED_INPLACE) -E 's!$(IMPORT_PREFIX)(bytes|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)!\1!g' $(GO_SOURCES)
	gofmt -s -w $(GO_SOURCES)
	touch $@

GW_SOURCES_TARGET := $(LOCAL_BIN)/.gw_protobuf_sources
$(GW_SOURCES_TARGET): $(PROTOC) $(GW_SERVER_PROTOS) $(GW_TS_PROTOS) $(GO_PROTOS) $(GOGOPROTO_PROTO)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.gw.go' | xargs rm -f)
	$(PROTOC) -I.:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:. $(GW_SERVER_PROTOS)
	$(PROTOC) -I.:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH):$(COREOS_PATH):$(GRPC_GATEWAY_GOOGLEAPIS_PATH) --grpc-gateway_out=logtostderr=true,request_context=true:. $(GW_TS_PROTOS)
	touch $@

CPP_SOURCES_TARGET := $(LOCAL_BIN)/.cpp_protobuf_sources
$(CPP_SOURCES_TARGET): $(PROTOC) $(CPP_PROTOS)
	(cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.pb.h' '*.pb.cc' | xargs rm -f)
	mkdir -p $(NATIVE_ROOT)
	$(PROTOC) -I.:$(GOGO_PROTOBUF_PATH):$(PROTOBUF_PATH) --cpp_out=lite:$(NATIVE_ROOT) $(CPP_PROTOS)
	$(SED_INPLACE) -E '/gogoproto/d' $(CPP_HEADERS) $(CPP_SOURCES)
	touch $@

$(UI_JS): $(GO_PROTOS) $(COREOS_RAFT_PROTOS) $(YARN_INSTALLED_TARGET)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBJS) -t static-module -w es6 --strict-long --keep-case --path $(ORG_ROOT) --path $(GOGO_PROTOBUF_PATH) --path $(COREOS_PATH) --path $(GRPC_GATEWAY_GOOGLEAPIS_PATH) $(GW_PROTOS) >> $@

$(UI_TS): $(UI_JS) $(YARN_INSTALLED_TARGET)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' > $@
	$(PBTS) $(UI_JS) >> $@

.DEFAULT_GOAL := protos
.PHONY: protos
protos: $(GO_SOURCES_TARGET) $(GW_SOURCES_TARGET) $(CPP_SOURCES_TARGET) $(UI_SOURCES)

.PHONY: clean
clean:
	rm -rf $(GO_SOURCES_TARGET) $(GW_SOURCES_TARGET) $(CPP_SOURCES_TARGET) $(UI_SOURCES)
