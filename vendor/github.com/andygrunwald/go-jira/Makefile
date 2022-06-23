.DEFAULT_GOAL := help

.PHONY: help
help: ## Outputs the help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: test
test: ## Runs all unit, integration and example tests.
	go test -race -v ./...

.PHONY: vet
vet: ## Runs go vet (to detect suspicious constructs).
	go vet ./...

.PHONY: fmt
fmt: ## Runs go fmt (to check for go coding guidelines).
	gofmt -d -s .

.PHONY: staticcheck
staticcheck: ## Runs static analysis to prevend bugs, foster code simplicity, performance and editor integration.
	go get -u honnef.co/go/tools/cmd/staticcheck
	staticcheck ./...

.PHONY: all
all: test vet fmt staticcheck ## Runs all source code quality targets (like test, vet, fmt, staticcheck)
