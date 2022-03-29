export GOPATH = $(CURDIR)
export BUILDCHANNEL := source-archive

.PHONY: all build buildbsl check clean install test
all build buildbsl check clean install test:
	$(MAKE) -C src/github.com/cockroachdb/cockroach $@
