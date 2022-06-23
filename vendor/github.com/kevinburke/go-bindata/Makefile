SHELL = /bin/bash -o pipefail

BENCHSTAT := $(GOPATH)/bin/benchstat
DIFFER := $(GOPATH)/bin/differ
MEGACHECK := $(GOPATH)/bin/megacheck
WRITE_MAILMAP := $(GOPATH)/bin/write_mailmap

UNAME := $(shell uname)

all:
	$(MAKE) -C testdata

$(DIFFER):
ifeq ($(UNAME), Darwin)
	curl --silent --location --output $(GOPATH)/bin/differ https://github.com/kevinburke/differ/releases/download/0.5/differ-darwin-amd64 && chmod 755 $(GOPATH)/bin/differ
endif
ifeq ($(UNAME), Linux)
	curl --silent --location --output $(GOPATH)/bin/differ https://github.com/kevinburke/differ/releases/download/0.5/differ-linux-amd64 && chmod 755 $(GOPATH)/bin/differ
endif

diff-testdata: | $(DIFFER)
	$(DIFFER) $(MAKE) -C testdata
	$(DIFFER) go fmt ./testdata/out/...

$(MEGACHECK):
	go get honnef.co/go/tools/cmd/megacheck

lint: | $(MEGACHECK)
	go vet ./...
	$(MEGACHECK) ./...

go-test:
	go test ./...

go-race-test:
	go test -race ./...

test: go-test
	$(MAKE) -C testdata

race-test: lint go-race-test
	$(MAKE) -C testdata

$(GOPATH)/bin/go-bindata:
	go install -v ./...

$(BENCHSTAT):
	go get golang.org/x/perf/cmd/benchstat

bench: $(GOPATH)/bin/go-bindata | $(BENCHSTAT)
	go list ./... | grep -v vendor | xargs go test -benchtime=5s -bench=. -run='^$$' 2>&1 | $(BENCHSTAT) /dev/stdin

$(WRITE_MAILMAP):
	go get -u github.com/kevinburke/write_mailmap

force: ;

AUTHORS.txt: force | $(WRITE_MAILMAP)
	$(WRITE_MAILMAP) > AUTHORS.txt

authors: AUTHORS.txt

ci: lint go-race-test diff-testdata

# Ensure you have updated go-bindata/version.go manually.
release: | $(RELEASE) race-test diff-testdata
ifndef version
	@echo "Please provide a version"
	exit 1
endif
	git push origin master
	git push origin --tags
