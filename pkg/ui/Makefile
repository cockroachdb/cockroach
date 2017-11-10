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

GOBINDATA_TARGET = embedded.go

STYLINT            := ./node_modules/.bin/stylint
TSLINT             := ./node_modules/.bin/tslint
KARMA              := ./node_modules/.bin/karma
WEBPACK            := ./node_modules/.bin/webpack
WEBPACK_DEV_SERVER := ./node_modules/.bin/webpack-dev-server
WEBPACK_DASHBOARD  := ./opt/node_modules/.bin/webpack-dashboard

.DEFAULT_GOAL := all
.PHONY: all
all: lint test $(GOBINDATA_TARGET)

.PHONY: protos
protos:
	$(MAKE) -C $(REPO_ROOT) -f build/protobuf.mk

# Running `make generate` from the root will call this target via `go generate`.
# Update this if you add new generated files.
.PHONY: generate
generate: $(GOBINDATA_TARGET)

.PHONY: lint
lint: | protos
	$(NODE_RUN) $(STYLINT) -c .stylintrc styl
	$(NODE_RUN) $(TSLINT) -c tslint.json -p tsconfig.json --type-check
	@# TODO(benesch): Invoke tslint just once when palantir/tslint#2827 is fixed.
	$(NODE_RUN) $(TSLINT) -c tslint.json *.js
	@if $(NODE_RUN) yarn list | grep phantomjs; then echo ^ forbidden UI dependency >&2; exit 1; fi

# See "DLLs for speedy builds" in the README for details.
DLLS := dist/protos.dll.js dist/vendor.dll.js protos-manifest.json vendor-manifest.json

.PHONY: test
test: $(DLLS)
	$(NODE_RUN) $(KARMA) start

.PHONY: test-watch
test-watch: $(DLLS)
	$(NODE_RUN) $(KARMA) start --no-single-run --auto-watch

# (Ab)use a pattern rule to teach Make that this one command produces two files.
# Normally, it would run the recipe twice if dist/FOO.js and FOO-manifest.js
# were both out-of-date.
#
# See: https://stackoverflow.com/a/3077254/1122351
dist/%.dll.js %-manifest.json: webpack.%.js | protos
	$(NODE_RUN) $(WEBPACK) -p --config $<

$(GOBINDATA_TARGET): webpack.app.js $(DLLS) $(shell find src styl)
	find dist -mindepth 1 -not -name '*.dll.js' -delete
	$(NODE_RUN) $(WEBPACK) --config $<
	go-bindata -nometadata -pkg ui -o $@ -prefix dist dist/...
	# Add comment recognized by reviewable.
	echo '// GENERATED FILE DO NOT EDIT' >> $@
	gofmt -s -w $@
	goimports -w $@

yarn.opt.installed:
	$(NODE_RUN) -C opt yarn install
	touch $@

.PHONY: watch
watch: export TARGET := http://localhost:8080
watch: PORT := 3000
watch: $(DLLS) yarn.opt.installed
	$(WEBPACK_DASHBOARD) -- $(WEBPACK_DEV_SERVER) --config webpack.app.js --port $(PORT)

.PHONY: maintainer-clean
maintainer-clean:
	rm -rf dist node_modules $(DLLS) $(GOBINDATA_TARGET) $(YARN_INSTALLED_TARGET)
