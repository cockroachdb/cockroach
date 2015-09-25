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

REPO_ROOT    := $(realpath ..)
ORG_ROOT     := $(REPO_ROOT)/..
GITHUB_ROOT  := $(ORG_ROOT)/..
NODE_MODULES := node_modules
TS_ROOT      := ts
STYL_ROOT    := styl
TEST_ROOT    := ts/test

# Ensure we only have one entry in GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN := $(GOPATH)/bin

GO_BINDATA := $(GOPATH_BIN)/go-bindata

TYPESCRIPT_TARGET      := build/js/app.js
TEST_TARGET            := test/js/test.js
CSS_TARGET             := build/css/app.css
GOBINDATA_TARGET       := ./embedded.go
GOBINDATA_DEBUG_TARGET := ./embedded_debug.go

.PHONY:
all: $(GOBINDATA_TARGET) $(TEST_TARGET)

.PHONY:
debug: $(GOBINDATA_DEBUG_TARGET)

npm.installed: npm-shrinkwrap.json
	npm install -g bower gulp tsd
	npm install
	touch $@

$(TYPESCRIPT_TARGET): npm.installed
	gulp release

$(TEST_TARGET): npm.installed
	gulp build:test

$(GOBINDATA_TARGET): npm.installed
	gulp release

$(GOBINDATA_DEBUG_TARGET): npm.installed
	gulp debug
