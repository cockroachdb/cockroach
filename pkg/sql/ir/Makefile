# Trigger these rules by running `make generate PKG=./pkg/sql/ir/irgen` from the
# repository root to ensure your PATH includes vendored binaries.

SHELL = /usr/bin/env bash

TEMPLATES = base/base.go base/sexpr.go
DEFS = example
TARGETS = $(foreach E,$(DEFS),$(foreach T,$(TEMPLATES),$(E)/$(T:.go=.ir.go)))
all: $(TARGETS)

clean:
	rm -f $(TARGETS)
	rm -f irgen/irgen

irgen/irgen: $(shell find irgen -name \*.go)
	cd irgen && go build

.SUFFIXES: .ir.go

# example/base/base.ir.go -> example
ir_def_base = $(firstword $(subst /, ,$(1)))

# example/base/base.ir.go -> base/base.go
template_path = $(subst .ir,,$(subst $(call ir_def_base,$(1))/,,$(1)))

# The special phrase ".SECONDEXPANSION" allows one to express rule
# dependencies as a function of the name of the target.
.SECONDEXPANSION:

# The dependencies in the following rules are exactly, in this order:
# - the name of the generator program ./irgen,
# - the source template,
# - the input definition file
# Those three items are then taken together to construct
# a valid command line with $^
%.ir.go: irgen/irgen $$(call template_path,$$@) $$(call ir_def_base,$$@).def
	$^ > $@.tmp
	mv -f $@.tmp $@
	gofmt -s -w $@
