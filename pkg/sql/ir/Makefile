# Trigger these rules by running `make generate PKG=./pkg/sql/ir/irgen` from the
# repository root to ensure your PATH includes vendored binaries.

SHELL = /usr/bin/env bash

TEMPLATES = base/base.go base/sexpr.go

TARGETS = example

all: $(foreach E,$(TARGETS),$(foreach T,$(TEMPLATES),$(E)/$(T:.go=.ir.go)))

irgen/irgen: $(shell find irgen -name \*.go)
	cd irgen && go build

.SUFFIXES: .ir.go

# The special phrase ".SECONDEXPANSION" allows one to express rule
# dependencies as a function of the name of the target.
.SECONDEXPANSION:

# The dependencies in the following rules are exactly, in this order:
# - the name of the generator program ./irgen,
# - the source template,
# - the input definition file
# Those three items are then taken together to construct
# a valid command line with $^
%.ir.go: irgen/irgen $$(subst .ir,,$$(subst $$(firstword $$(subst /, ,$$@))/,,$$@)) $$(firstword $$(subst /, ,$$@)).def
	$^ > $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@
