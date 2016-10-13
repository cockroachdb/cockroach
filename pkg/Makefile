PKG=...

%:
	@make -C .. $(MAKECMDGOALS) PKG=$(addprefix ./pkg/,$(PKG)) $(filter-out PKG=%,$(MAKEFLAGS))
