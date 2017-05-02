# This is a convenience Makefile which defers all real work to
# ../Makefile. The .DEFAULT rule is run for any target specified on
# the command line. We use the builtin MAKECMDGOALS and MAKEFLAGS
# variables (the command line targets and flags respectively) to
# perform our recursive make invocation. Lastly, we take care to make
# any PKG specification relative to "./pkg".

# Any file/directory in the current directory needs to be explicitly
# marked as .PHONY, otherwise make tries to bring it up to date. This
# is needed for the "acceptance" and "build" targets.
DIRS=$(filter-out Makefile,$(wildcard *))

.PHONY: $(DIRS)
.DEFAULT $(DIRS):
	@$(MAKE) -C .. $(MAKECMDGOALS) \
	  $(patsubst PKG=%,PKG=./pkg/%,$(filter PKG=%,$(MAKEFLAGS)))
