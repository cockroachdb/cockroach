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

REPO_ROOT := ../..
include $(REPO_ROOT)/build/common.mk

GOBINDATA_TARGET = embedded.go

.DEFAULT_GOAL := all
.PHONY: all
all: lint test $(GOBINDATA_TARGET)

.PHONY: protos
protos:
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

# Running `go generate` will call this target. Update this if you add new
# generated files.
.PHONY: generate
generate: $(GOBINDATA_TARGET)

.PHONY: lint
lint: yarn.installed
	yarn run stylint -c .stylintrc styl
	yarn run tslint -c tslint.json $(shell find src -name '*.tsx' -or -name '*.ts' -not -path 'src/js/*')

.PHONY: test
test: yarn.installed protos
	yarn run karma start

.PHONY: test-debug
test-debug: yarn.installed protos
	yarn run karma start --browsers Chrome --no-single-run --reporters mocha

$(GOBINDATA_TARGET): yarn.installed protos
	rm -rf dist
	yarn run webpack -p
	go-bindata -nometadata -pkg ui -o $@ -prefix dist dist
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@
