# Copyright 2017 The Cockroach Authors.
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
