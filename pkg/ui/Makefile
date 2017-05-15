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

# Running `make generate` from the root will call this target via `go generate`.
# Update this if you add new generated files.
.PHONY: generate
generate: $(GOBINDATA_TARGET)

.PHONY: lint
lint: $(YARN_INSTALLED_TARGET) | protos
	stylint -c .stylintrc styl
	tslint -c tslint.json -p tsconfig.json --type-check

.PHONY: test
test: $(YARN_INSTALLED_TARGET) | protos
	karma start

.PHONY: test-debug
test-debug: $(YARN_INSTALLED_TARGET) | protos
	karma start --browsers Chrome --no-single-run --reporters mocha

$(GOBINDATA_TARGET): $(YARN_INSTALLED_TARGET) $(shell git ls-files | grep -vF $(GOBINDATA_TARGET)) | protos
	rm -rf dist
	webpack -p
	go-bindata -nometadata -pkg ui -o $@ -prefix dist dist/...
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@
