# Copyright 2017 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This is a convenience Makefile which defers all real work to the top-level
# Makefile. The % rule is run for any target specified on the command line. We
# use the builtin MAKECMDGOALS (the command line targets) to perform our
# recursive make invocation. Lastly, we take care to give all specified goals
# a "ui-" prefix.
#
# We use a level of indirection through the "default" rule so that specifying
# multiple targets on the command line (e.g. "make test lint") does not result
# in multiple invocations of the rule.

MAKECMDGOALS ?= lint test $(GOBINDATA_TARGET)

%: default ;

.PHONY: default
default:
	@$(MAKE) -C ../.. $(addprefix ui-,$(MAKECMDGOALS))
