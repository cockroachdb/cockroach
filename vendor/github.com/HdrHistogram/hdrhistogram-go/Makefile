# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt
GODOC=godoc

.PHONY: all test coverage
all: test

checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

lint:
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run

get:
	$(GOGET) -v ./...

fmt:
	$(GOFMT) ./...

test: get fmt
	$(GOTEST) -count=1 ./...

coverage: get test
	$(GOTEST) -count=1 -race -coverprofile=coverage.txt -covermode=atomic .

benchmark: get
	$(GOTEST) -bench=. -benchmem

godoc:
	$(GODOC)

