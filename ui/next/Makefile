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
PATH          := ../node_modules/.bin:$(PATH)

NODE_MODULES  := node_modules
JSPM_PACKAGES := jspm_packages
TYPINGS       := typings
TS_ROOT       := app

REMOTE_DEPS   := npm.installed jspm.installed typings.installed

.PHONY: all
all: lint $(REMOTE_DEPS)

.PHONY: lint
lint: npm.installed
	tslint -c $(TS_ROOT)/tslint.json $(shell find $(TS_ROOT) -name '*.tsx' -or -name '*.ts')

npm.installed: npm-shrinkwrap.json
	rm -rf $(NODE_MODULES)/ $(TYPINGS)/
	npm install --progress=false
	touch $@

typings.installed: typings.json npm.installed
	rm -rf $(TYPINGS)/
	typings install
	touch $@

jspm.installed: npm.installed
	rm -rf $(JSPM_PACKAGES)/
	jspm install
	touch $@
