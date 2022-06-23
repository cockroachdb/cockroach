export GO111MODULE=on

build: build/geohash

build/geohash:
	go build -o build/geohash ./cmd/geohash

.PHONY: test
test:
	go test ./...

.PHONY: lint
lint: \
	golangci-lint

GOLANGCI_LINT_VERSION=v1.17.0
GOLANGCI_LINT_DIR=$(shell go env GOPATH)/pkg/golangci-lint/$(GOLANGCI_LINT_VERSION)
$(GOLANGCI_LINT_DIR):
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(GOLANGCI_LINT_DIR) $(GOLANGCI_LINT_VERSION)

.PHONY: install-golangci-lint
install-golangci-lint: $(GOLANGCI_LINT_DIR)

.PHONY: golangci-lint
golangci-lint: install-golangci-lint
	$(GOLANGCI_LINT_DIR)/golangci-lint run

.PHONY: clean
clean:
	rm -rf build
