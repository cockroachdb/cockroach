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

# NOTE: for some reason bash is necessary for updating the PATH to work
# See http://stackoverflow.com/questions/8941110/how-i-could-add-dir-to-path-in-makefile
SHELL            := /bin/bash

# Update the path to prefer binstubs over globals
PATH                 := node_modules/.bin:$(PATH)

REPO_ROOT            := $(realpath ..)
ORG_ROOT             := $(REPO_ROOT)/..
GITHUB_ROOT          := $(ORG_ROOT)/..
BOWER_COMPONENTS     := bower_components
NODE_MODULES         := node_modules
TYPINGS              := typings
TS_ROOT              := ts
STYL_ROOT            := styl
TEST_ROOT            := ts/test
GENERATED_ROOT       := generated

# Ensure we only have one entry in GOPATH
GOPATH := $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN := $(GOPATH)/bin

GO_BINDATA := $(GOPATH_BIN)/go-bindata

TYPESCRIPT_TARGET       := build/app.js
TEST_TARGET             := build/test.js
CSS_TARGET              := build/app.css
CSS_DEBUG_TARGET        := build/app_debug.css
REMOTE_DEPS             := npm.installed bower.installed typings.installed
INDEX                   := index.html
GOBINDATA_FONTS         := ./fonts/*
GOBINDATA_ASSETS        := ./assets/*
GOBINDATA_DEPS          := $(TYPESCRIPT_TARGET) $(CSS_TARGET)
GOBINDATA_DEBUG_DEPS    := $(TYPESCRIPT_TARGET) $(CSS_DEBUG_TARGET)
GOBINDATA_SOURCES       := $(BOWER_COMPONENTS)/d3/d3.min.js \
                           $(NODE_MODULES)/lodash/lodash.min.js \
                           $(BOWER_COMPONENTS)/mithriljs/mithril.js \
                           $(BOWER_COMPONENTS)/nvd3/build/nv.d3.min.css \
                           $(BOWER_COMPONENTS)/nvd3/build/nv.d3.min.js \
                           $(BOWER_COMPONENTS)/moment/min/moment.min.js \
                           $(BOWER_COMPONENTS)/moment-timezone/builds/moment-timezone-with-data.min.js \
                           $(GOBINDATA_FONTS) \
                           $(GOBINDATA_ASSETS) \
                           favicon.ico \
                           apple-touch-icon.png
GOBINDATA_DEBUG_SOURCES := $(BOWER_COMPONENTS)/d3/d3.js \
                           $(NODE_MODULES)/lodash/lodash.js \
                           $(BOWER_COMPONENTS)/mithriljs/mithril.js \
                           $(BOWER_COMPONENTS)/nvd3/build/nv.d3.css \
                           $(BOWER_COMPONENTS)/nvd3/build/nv.d3.js \
                           $(BOWER_COMPONENTS)/moment/moment.js \
                           $(BOWER_COMPONENTS)/moment-timezone/builds/moment-timezone-with-data.js \
                           $(GOBINDATA_FONTS) \
                           $(GOBINDATA_ASSETS) \
                           favicon.ico \
                           apple-touch-icon.png
GOBINDATA_TARGET        := ./embedded.go
GOBINDATA_DEBUG_TARGET  := ./embedded_debug.go

.PHONY: all
all: lint $(GOBINDATA_TARGET) $(TEST_TARGET)
	rm -f $(GOBINDATA_DEBUG_TARGET)

.PHONY: lint
lint: npm.installed
	stylint -c .stylintrc $(STYL_ROOT)
	tslint -c $(TS_ROOT)/tslint.json $(shell find $(TS_ROOT) -name '*.ts')

.PHONY: debug
debug: $(GOBINDATA_DEBUG_TARGET) $(TEST_TARGET)
	rm -f $(GOBINDATA_TARGET)

.PHONY: next
next: $(CSS_TARGET)
	make -C next

bower.installed: bower.json npm.installed
	rm -rf $(BOWER_COMPONENTS)/
	bower install --config.interactive=false --allow-root
	touch $@

npm.installed: npm-shrinkwrap.json
	# TODO(tamird): DRY this with `clean`?
	rm -rf $(BOWER_COMPONENTS)/ $(NODE_MODULES)/ $(TYPINGS)/
	npm install --progress=false
	touch $@

typings.installed: typings.json npm.installed
	rm -rf $(TYPINGS)/
	typings install
	touch $@

$(TYPESCRIPT_TARGET): $(shell find $(TS_ROOT)) $(REMOTE_DEPS)
	tsc -p $(TS_ROOT)

$(TEST_TARGET): $(shell find $(TS_ROOT)) $(REMOTE_DEPS)
	tsc -p $(TEST_ROOT)

$(CSS_TARGET): $(shell find $(STYL_ROOT)) npm.installed bower.installed
	stylus $(STYL_ROOT)/app.styl \
	       --compress \
	       --include-css \
	       --use nib \
	       --out $@

$(CSS_DEBUG_TARGET): $(shell find $(STYL_ROOT)) npm.installed bower.installed
	stylus $(STYL_ROOT)/app.styl \
	       --include-css \
	       --use nib \
	       --out $@

$(GOBINDATA_TARGET): $(GOBINDATA_DEPS) bower.installed release/$(INDEX)
	rm -f $(INDEX)
	cp release/$(INDEX) $(INDEX)
	chmod a-w $(INDEX)
	$(GO_BINDATA) -nometadata -pkg ui -o $@ $(GOBINDATA_DEPS) $(INDEX) $(GOBINDATA_SOURCES)
	gofmt -s -w $@
	goimports -w $@

$(GOBINDATA_DEBUG_TARGET): $(GOBINDATA_DEBUG_DEPS) bower.installed debug/$(INDEX)
	rm -f $(INDEX)
	cp debug/$(INDEX) $(INDEX)
	chmod a-w $(INDEX)
	$(GO_BINDATA) -nometadata -pkg ui -o $@ -debug $(GOBINDATA_DEBUG_DEPS) $(INDEX) $(GOBINDATA_DEBUG_SOURCES)

watch: debug
	gulp watch

.PHONY: clean
clean:
	rm -f $(REMOTE_DEPS) $(INDEX)
	rm -rf $(BOWER_COMPONENTS) $(NODE_MODULES) $(TYPINGS) build generated
