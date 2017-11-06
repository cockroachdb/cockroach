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

REPO_ROOT := ../..
include $(REPO_ROOT)/build/common.mk

GOBINDATA_OSS_TARGET = embedded.go
GOBINDATA_CCL_TARGET = embedded.ccl.go

STYLINT            := $(NODE_RUN) ./node_modules/.bin/stylint
TSLINT             := $(NODE_RUN) ./node_modules/.bin/tslint
KARMA              := $(NODE_RUN) ./node_modules/.bin/karma
WEBPACK            := $(NODE_RUN) ./node_modules/.bin/webpack
WEBPACK_DEV_SERVER := $(NODE_RUN) ./node_modules/.bin/webpack-dev-server

.DEFAULT_GOAL := all
.PHONY: all
all: lint test generate-ccl

.PHONY: protos
protos:
	$(MAKE) -C $(ORG_ROOT) -f cockroach/build/protobuf.mk

# Running `make ui` from the root will call one of these targets.
# Update these if you add new generated files.
.PHONY: generate-ccl generate-oss
generate-ccl: $(GOBINDATA_CCL_TARGET)
generate-oss: $(GOBINDATA_OSS_TARGET)

.PHONY: lint
lint: | protos
	$(STYLINT) -c .stylintrc styl
	$(TSLINT) -c tslint.json -p tsconfig.json --type-check
	@# TODO(benesch): Invoke tslint just once when palantir/tslint#2827 is fixed.
	$(TSLINT) -c tslint.json *.js

# See "DLLs for speedy builds" in the README for details.
DLLS := dist/protos.dll.js dist/vendor.dll.js protos-manifest.json vendor-manifest.json

.PHONY: test
test: $(DLLS)
	$(KARMA) start

.PHONY: test-watch
test-watch: $(DLLS)
	$(KARMA) start --no-single-run --auto-watch

# (Ab)use a pattern rule to teach Make that this one command produces two files.
# Normally, it would run the recipe twice if dist/FOO.js and FOO-manifest.js
# were both out-of-date.
#
# See: https://stackoverflow.com/a/3077254/1122351
dist/%.dll.js %-manifest.json: webpack.%.js | protos
	$(WEBPACK) -p --config $<

$(GOBINDATA_OSS_TARGET): webpack.oss.js $(DLLS) $(shell find src styl)
	find dist -mindepth 1 -not -name '*.dll.js' -delete
	$(WEBPACK) --config $<
	go-bindata -nometadata -pkg ui -o $@ -tags "!ccl" -prefix dist dist/...
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@

$(GOBINDATA_CCL_TARGET): webpack.ccl.js $(DLLS) $(shell find src styl)
	find dist -mindepth 1 -not -name '*.dll.js' -delete
	$(WEBPACK) --config $<
	go-bindata -nometadata -pkg ui -o $@ -tags ccl -prefix dist dist/...
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@

.PHONY: watch watchoss
watch watchoss: export TARGET := http://localhost:8080
watch watchoss: PORT := 3000
watch: $(DLLS)
	$(WEBPACK_DEV_SERVER) --config webpack.ccl.js --port $(PORT)
watchoss: $(DLLS)
	$(WEBPACK_DEV_SERVER) --config webpack.oss.js --port $(PORT)

.PHONY: maintainer-clean
maintainer-clean:
	rm -rf dist node_modules $(DLLS) $(GOBINDATA_TARGET) $(YARN_INSTALLED_TARGET)
