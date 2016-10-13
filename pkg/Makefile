# This is a convenience Makefile which defers all real work to
# ../Makefile. The .DEFAULT rule is run for any target specified on
# the command line. We use the builtin MAKECMDGOALS and MAKEFLAGS
# variables (the command line targets and flags respectively) to
# perform our recursive make invocation. Lastly, we take care to make
# any PKG specification relative to "./pkg".

.DEFAULT:
	@make -C .. $(MAKECMDGOALS) \
	  $(patsubst PKG=%,PKG=./pkg/%,$(filter PKG=%,$(MAKEFLAGS))) \
	  $(filter-out PKG=%,$(MAKEFLAGS))
