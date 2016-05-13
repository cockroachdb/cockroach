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
SHELL         := /bin/bash

# Update the path to prefer binstubs over globals
PATH          := $(shell npm bin):$(PATH)

NODE_MODULES  := node_modules
JSPM_PACKAGES := jspm_packages
TYPINGS       := typings
TS_ROOT       := app

REMOTE_DEPS   := npm.installed jspm.installed typings.installed

.PHONY: all
all: lint $(REMOTE_DEPS) app/js/protos.js generated/protos.d.ts

.PHONY: lint
lint: npm.installed typings.installed
	tslint -c tslint.json $(shell find $(TS_ROOT) -name '*.tsx' -or -name '*.ts')

npm.installed: package.json npm-shrinkwrap.json
	rm -rf $(NODE_MODULES)/ $(TYPINGS)/
	npm install --no-progress
	touch $@

typings.installed: typings.json npm.installed
	typings install
	typings prune
	touch $@

jspm.installed: npm.installed
	rm -rf $(JSPM_PACKAGES)/
	jspm install
	touch $@

PBJS_ARGS = --path ../../../ ../../server/status.proto ../../server/status/status.proto ../../ts/timeseries.proto

app/js/protos.js: npm.installed
	pbjs -t commonjs $(PBJS_ARGS) > $@

generated/protos.json: npm.installed
	pbjs $(PBJS_ARGS) > $@

generated/protos.d.ts: generated/protos.json
	proto2ts --file generated/protos.json > $@

.PHONY: test
test: npm.installed typings.installed
	mocha $(TS_ROOT)/'**/*.spec.ts*' --require ts-node/register
