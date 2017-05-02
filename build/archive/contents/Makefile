PREFIX := /usr/local
BINDIR := $(PREFIX)/bin
INSTALL := install

.PHONY: all build buildoss test
all build buildoss test:
	$(MAKE) -C src/github.com/cockroachdb/cockroach $@

.PHONY: install
install: build
	$(INSTALL) -d -m 755 $(DESTDIR)$(BINDIR)
	$(INSTALL) -m 755 src/github.com/cockroachdb/cockroach/cockroach $(DESTDIR)$(BINDIR)
