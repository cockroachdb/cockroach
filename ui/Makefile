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

TYPESCRIPT_TARGET      := js/app.js
TEST_TARGET            := js/test.js
CSS_TARGET             := css/app.css
STYL_OPTIONS           :=
GOBINDATA_SOURCES      := $(TYPESCRIPT_TARGET) index.html $(shell find css js/libs -type f)
GOBINDATA_TARGET       := ./embedded.go
GOBINDATA_DEBUG_TARGET := ./embedded_debug.go

.PHONY:
all: STYL_OPTIONS += --compress
all: $(GOBINDATA_TARGET) $(TEST_TARGET)
	rm -f $(GOBINDATA_DEBUG_TARGET)

.PHONY:
debug: $(GOBINDATA_DEBUG_TARGET)
	rm -f $(GOBINDATA_TARGET)

npm.installed: npm-shrinkwrap.json
	npm install
	touch $@

$(TYPESCRIPT_TARGET): $(shell find $(TS_ROOT)) npm.installed
	$(NODE_MODULES)/.bin/tsc -p $(TS_ROOT)
	$(NODE_MODULES)/.bin/tslint -c $(TS_ROOT)/tslint.json $(shell find $(TS_ROOT) -name '*.ts' -not -path '*typings*')

$(TEST_TARGET): $(shell find $(TS_ROOT)) npm.installed
	$(NODE_MODULES)/.bin/tsc -p $(TEST_ROOT)

$(CSS_TARGET): $(shell find $(STYL_ROOT)) npm.installed
	$(NODE_MODULES)/.bin/stylus $(STYL_OPTIONS) $(STYL_ROOT)/app.styl --out css

$(GOBINDATA_TARGET): $(GOBINDATA_SOURCES)
	$(GO_BINDATA) -mode 0644 -modtime 1400000000 -pkg ui -o $@ $^
	gofmt -s -w $@
	goimports -w $@

$(GOBINDATA_DEBUG_TARGET): $(GOBINDATA_SOURCES)
	$(GO_BINDATA) -pkg ui -o $@ -debug $^
