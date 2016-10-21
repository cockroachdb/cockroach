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

# Prefer tools from node_modules over those elsewhere on the path.
# This ensures that we get the versions pinned in yarn.lck.
export PATH := $(shell yarn bin):$(PATH)
# HACK: Make has a fast path and a slow path for command execution,
# but the fast path uses the PATH variable from when make was started,
# not the one we set on the previous line. In order for the above
# line to have any effect, we must force make to always take the slow path.
# Setting the SHELL variable to a value other than the default (/bin/sh)
# is one way to do this globally.
# http://stackoverflow.com/questions/8941110/how-i-could-add-dir-to-path-in-makefile/13468229#13468229
export SHELL := $(shell which bash)
ifeq ($(SHELL),)
$(error bash is required)
endif

REPO_ROOT   = $(realpath ../..)
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
all: lint test $(GOBINDATA_TARGET)

# Running `go generate` will call this target. Update this if you add new
# generated files.
.PHONY: generate
generate: $(GOBINDATA_TARGET)

# TODO(tamird): is there a way to not repeat this here? It's already in protobuf.mk
app/js/protos.js generated/protos.json generated/protos.d.ts: $(addprefix $(REPO_ROOT)/, $(sort $(shell cd $(REPO_ROOT) && git ls-files --exclude-standard --cached --others -- '*.proto')))
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

.PHONY: lint
lint: yarn.installed typings.installed
	stylint -c .stylintrc $(STYL_ROOT)
	tslint -c tslint.json $(shell find $(TS_ROOT) -name '*.tsx' -or -name '*.ts')

.PHONY: test
test: yarn.installed typings.installed jspm.installed
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

.PHONY: livereload
livereload:
	chokidar-socket-emitter

# TODO(tamird/matt/max): write a systemjs stylus plugin.
.PHONY: watch
watch: STYLFLAGS += --watch
watch: $(CSS_TARGET)

$(CSS_TARGET): STYLFLAGS += --include-css
$(CSS_TARGET): STYLFLAGS += --use nib
$(CSS_TARGET): $(shell find $(STYL_ROOT)) yarn.installed
	stylus $(STYL_ROOT)/app.styl $(STYLFLAGS) --out $@

yarn.installed: package.json yarn.lock
	yarn install
	touch $@

typings.installed: generated/protos.d.ts typings.json yarn.installed jspm.installed
	typings install
	typings prune
	touch $@

jspm.installed: yarn.installed
	jspm install
	touch $@
