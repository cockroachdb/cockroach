# Copyright 2022 The Cockroach Authors.
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

all: build
	$(MAKE) help

help:
	@echo
	@echo "Tip: use ./dev instead of 'make'."
	@echo "Try:"
	@echo "    ./dev help"
	@echo

# Generic build rules.
.PHONY: build build%
build: doctor
	./dev build $(TARGET)
# Alias: buildshort -> build short; buildoss -> build oss; buildtests -> build tests etc.
build%: doctor
	./dev build $(@:build%=%)

.PHONY: doctor
doctor:
	./dev doctor

# Most common rules.
.PHONY: generate test bench
generate test bench: doctor
	./dev $@ $(TARGET)

# Documented clean-all rules.
.PHONY: clean
clean: doctor
	./dev ui clean --all
	bazel clean --expunge

# Documented clean-everything rule (dangerous: removes working tree edits!)
.PHONY: unsafe-clean
unsafe-clean: clean
	git clean -dxf

## Indicate the base root directory where to install.
## Can point e.g. to a container root.
DESTDIR      :=
## The target tree inside DESTDIR.
prefix       := /usr/local
## The target bin directory inside the target tree.
bindir       := $(prefix)/bin
libdir       := $(prefix)/lib
## The install program.
INSTALL      := install

TARGET_TRIPLE := $(shell $(shell go env CC) -dumpmachine)
target-is-windows := $(findstring w64,$(TARGET_TRIPLE))
target-is-macos := $(findstring darwin,$(TARGET_TRIPLE))
DYN_EXT     := so
EXE_EXT     :=
ifdef target-is-macos
DYN_EXT     := dylib
endif
ifdef target-is-windows
DYN_EXT     := dll
EXE_EXT     := .exe
endif

.PHONY: install
install: build buildgeos
	: Install the GEOS library.
	$(INSTALL) -d -m 755 $(DESTDIR)$(libdir)
	$(INSTALL) -m 755 lib/libgeos.$(DYN_EXT) $(DESTDIR)$(libdir)/libgeos.$(DYN_EXT)
	$(INSTALL) -m 755 lib/libgeos_c.$(DYN_EXT) $(DESTDIR)$(libdir)/libgeos_c.$(DYN_EXT)
	: Install the CockroachDB binary.
	$(INSTALL) -d -m 755 $(DESTDIR)$(bindir)
	$(INSTALL) -m 755 cockroach$(EXE_EXT) $(DESTDIR)$(bindir)/cockroach$(EXE_EXT)
