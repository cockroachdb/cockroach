# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This is a convenience Makefile which defers all real work to
# ../Makefile. The % rule is run for any target specified on the
# command line. We use the builtin MAKECMDGOALS and MAKEFLAGS
# variables (the command line targets and flags respectively) to
# perform our recursive make invocation. Lastly, we take care to make
# any PKG specification relative to "./pkg".
#
# We use a level of indirection through the "default" rule so that
# specifying multiple targets on the command line (e.g. "make test
# lint") does not result in multiple invocations of the rule.
#
# The rule needs an action (i.e. "@true") because reasons (I'm not
# really sure why - Peter).
%: default
	@true

.PHONY: default
default:
	@$(MAKE) -C .. $(MAKECMDGOALS) \
	  $(patsubst PKG=%,PKG=./pkg/%,$(filter PKG=%,$(MAKEFLAGS)))
