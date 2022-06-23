default: fmt get update test lint

GO       := go
GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST   := $(GO) test -gcflags='-l' -p 3 -v -race -timeout 6m -coverprofile=profile.out -covermode=atomic

FILES    := $(shell find . -name '*.go' -type f -not -name '*.pb.go' -not -name '*_generated.go' -not -name '*_test.go')
TESTS    := $(shell find . -name '*.go' -type f -not -name '*.pb.go' -not -name '*_generated.go' -name '*_test.go')

get:
	$(GO) get ./...
	$(GO) mod verify
	$(GO) mod tidy

update:
	$(GO) get -u -v ./...
	$(GO) mod verify
	$(GO) mod tidy

fmt:
	gofmt -s -l -w $(FILES) $(TESTS)

lint:
	GOFLAGS="-tags=functional" golangci-lint run

test:
	$(GOTEST) ./...

.PHONY: test_functional
test_functional:
	$(GOTEST) -tags=functional ./...
