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

# Any file/directory in the current directory needs to be explicitly
# marked as .PHONY, otherwise make tries to bring it up to date. This
# is needed for the "acceptance" and "build" targets.
DIRS=$(filter-out Makefile,$(wildcard *))
.PHONY: $(DIRS)
$(DIRS): default
	@true
