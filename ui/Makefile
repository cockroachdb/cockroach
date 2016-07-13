# Copyright 2016 The Cockroach Authors.
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
SHELL = /bin/bash
# Update the path to prefer binstubs over globals
PATH := $(shell npm bin):$(PATH)

REPO_ROOT   = $(realpath ..)
ORG_ROOT    = $(REPO_ROOT)/..
GITHUB_ROOT = $(ORG_ROOT)/..

# Ensure we only have one entry in GOPATH
GOPATH = $(GITHUB_ROOT)/../..
#                        ^  ^~ GOPATH
#                        |~ GOPATH/src

GOPATH_BIN = $(GOPATH)/bin
GO_BINDATA = $(GOPATH_BIN)/go-bindata

NODE_MODULES  = node_modules
JSPM_PACKAGES = jspm_packages
TYPINGS       = typings
TS_ROOT       = app
STYL_ROOT     = styl

GOBINDATA_FONTS  = fonts
GOBINDATA_ASSETS = assets
CSS_TARGET       = build/app.css
JSPM_TARGET      = build/app.js
GOBINDATA_TARGET = embedded.go

.PHONY: all
all: protobuf lint test $(GOBINDATA_TARGET)

.PHONY: protobuf
protobuf:
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

.PHONY: lint
lint: npm.installed typings.installed
	stylint -c .stylintrc $(STYL_ROOT)
	tslint -c tslint.json $(shell find $(TS_ROOT) -name '*.tsx' -or -name '*.ts')

.PHONY: test
test: protobuf npm.installed typings.installed jspm.installed
	karma start

GOBINDATA_SOURCES = $(JSPM_TARGET) assets fonts favicon.ico apple-touch-icon.png release.html

$(GOBINDATA_TARGET): $(GO_BINDATA) $(GOBINDATA_SOURCES)
	$(GO_BINDATA) -nometadata -pkg ui -o $@ $(GOBINDATA_SOURCES)
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@

$(JSPM_TARGET): jspm.installed typings.installed $(CSS_TARGET) $(shell find $(TS_ROOT) -name '*.tsx' -or -name '*.ts' -or -name '*.js')
	jspm bundle-sfx app/app build/app.js

# TODO(tamird/matt/max): write a systemjs stylus plugin.
.PHONY: watch
watch: STYLFLAGS += --watch
watch: $(CSS_TARGET)

$(CSS_TARGET): STYLFLAGS += --include-css
$(CSS_TARGET): STYLFLAGS += --use nib
$(CSS_TARGET): $(shell find $(STYL_ROOT)) npm.installed
	stylus $(STYL_ROOT)/app.styl $(STYLFLAGS) --out $@

npm.installed: package.json npm-shrinkwrap.json
	rm -rf $(NODE_MODULES)
	npm install --no-progress
	touch $@

typings.installed: typings.json npm.installed
	typings install
	typings prune
	touch $@

jspm.installed: npm.installed
	jspm install
	# jspm touches package.json, so we need to bump npm.installed's timestamp
	# here to prevent endless pointless rebuilds. This `touch` will still result
	# in a spurious `typings install`, but that's extremely fast compared to `npm
	# install`.
	touch npm.installed
	touch $@
