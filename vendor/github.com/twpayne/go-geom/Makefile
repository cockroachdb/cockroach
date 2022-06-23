GO?=go
GOFUMPT_VERSION=$(shell awk '/GOFUMPT_VERSION:/ { print $$2 }' .github/workflows/main.yml)
GOLANGCI_LINT_VERSION=$(shell awk '/GOLANGCI_LINT_VERSION:/ { print $$2 }' .github/workflows/main.yml)

.PHONY: all
all: test lint

.PHONY: test
test:
	${GO} test ./...

.PHONY: coverage.out
coverage.out:
	${GO} test -covermode=count --coverprofile=$@ ./...

.PHONY: lint
lint: ensure-golangci-lint
	./bin/golangci-lint run

.PHONY: format
format: ensure-gofumpt
	find . -name \*.go | xargs ./bin/gofumpt -extra -w

.PHONY: generate
generate: ensure-goderive ensure-goyacc
	PATH=$$PATH:$(shell pwd)/bin ${GO} generate ./...

.PHONY: install-tools
install-tools: ensure-goderive ensure-gofumpt ensure-golangci-lint ensure-goyacc

.PHONY: ensure-goderive
ensure-goderive:
	if [ ! -x bin/goderive ] ; then \
		mkdir -p bin ; \
		GOBIN=$(shell pwd)/bin ${GO} install "github.com/awalterschulze/goderive@latest" ; \
	fi

.PHONY: ensure-gofumpt
ensure-gofumpt:
	if [ ! -x bin/gofumpt ] || ( ./bin/gofumpt --version | grep -Fqv "v${GOFUMPT_VERSION}" ) ; then \
		mkdir -p bin ; \
		GOBIN=$(shell pwd)/bin ${GO} install "mvdan.cc/gofumpt@v${GOFUMPT_VERSION}" ; \
	fi

.PHONY: ensure-golangci-lint
ensure-golangci-lint:
	if [ ! -x bin/golangci-lint ] || ( ./bin/golangci-lint --version | grep -Fqv "version ${GOLANGCI_LINT_VERSION}" ) ; then \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- v${GOLANGCI_LINT_VERSION} ; \
	fi

.PHONY: ensure-goyacc
ensure-goyacc:
	if [ ! -x bin/goyacc ] ; then \
		mkdir -p bin ; \
		GOBIN=$(shell pwd)/bin ${GO} install "golang.org/x/tools/cmd/goyacc@latest" ; \
	fi
