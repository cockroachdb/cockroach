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
# Author: Matt Tracy (matt@cockroachlabs.com)

GOBINDATA_PACKAGE := resource

GOBINDATA_TARGET := ./embedded.go

GOBINDATA_SOURCE := ui/...

GOBINDATA_FLAGS := -mode 0644 -modtime 1400000000

.PHONY: all
all: gobindata 

.PHONY: js
js:
	tsc -p ui/ts/

.PHONY: gobindata
gobindata: js
	go-bindata $(GOBINDATA_FLAGS) -pkg $(GOBINDATA_PACKAGE) -o $(GOBINDATA_TARGET) $(GOBINDATA_SOURCE)
	gofmt -s -w $(GOBINDATA_TARGET)
	goimports -w $(GOBINDATA_TARGET)

.PHONY: debug
debug: GOBINDATA_FLAGS += -debug
debug: gobindata
