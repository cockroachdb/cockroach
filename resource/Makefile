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

REPO_ROOT      := $(realpath ..)
ORG_ROOT       := $(REPO_ROOT)/..
GITHUB_ROOT    := $(ORG_ROOT)/..

# Ensure we only have one entry in GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN := $(GOPATH)/bin

GO_BINDATA := $(GOPATH_BIN)/go-bindata

TYPESCRIPT_SOURCES     := ui/ts
TYPESCRIPT_TARGET      := ui/js/app.js
GOBINDATA_TARGET       := ./embedded.go
GOBINDATA_DEBUG_TARGET := ./embedded_debug.go

.PHONY:
all: $(GOBINDATA_TARGET)
	rm -f $(GOBINDATA_DEBUG_TARGET)

.PHONY:
debug: $(GOBINDATA_DEBUG_TARGET)
	rm -f $(GOBINDATA_TARGET)

$(TYPESCRIPT_TARGET): $(TYPESCRIPT_SOURCES)
	tsc -p $(TYPESCRIPT_SOURCES)

$(GOBINDATA_TARGET): $(TYPESCRIPT_TARGET) $(GO_BINDATA)
	$(GO_BINDATA) -mode 0644 -modtime 1400000000 -pkg resource -o $@ ui/...
	gofmt -s -w $@
	goimports -w $@

$(GOBINDATA_DEBUG_TARGET): $(TYPESCRIPT_TARGET) $(GO_BINDATA)
	$(GO_BINDATA) -pkg resource -o $@ -debug ui/...
