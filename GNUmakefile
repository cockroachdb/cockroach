# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

.PHONY: all
all: build
	$(MAKE) help

.PHONY: help
help:
	@echo
	@echo "Tip: use ./dev instead of 'make'."
	@echo "Try:"
	@echo "    ./dev help"
	@echo

# Generic build rules.
.PHONY: build build%
build:
	./dev build $(TARGET)
# Alias: buildshort -> build short; buildoss -> build oss; buildtests -> build tests etc.
build%:
	./dev build $(@:build%=%)

.PHONY: doctor
doctor:
	./dev doctor

# Most common rules.
.PHONY: generate test bench
generate test bench:
	./dev $@ $(TARGET)

# Documented clean-all rules.
.PHONY: clean
clean:
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
