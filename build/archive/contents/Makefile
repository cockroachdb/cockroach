export GOPATH = $(CURDIR)
export BUILDCHANNEL := source-archive

.PHONY: all build buildoss check clean install test
all build buildoss check clean install test:
	$(MAKE) -C src/github.com/cockroachdb/cockroach $@
