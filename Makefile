
.PHONY: verify
verify: verify-gofmt verify-dep verify-lint verify-mod-tidy

.PHONY: verify-gofmt
verify-gofmt:
	PASSES="gofmt" ./scripts/test.sh

.PHONY: verify-dep
verify-dep:
	PASSES="dep" ./scripts/test.sh

.PHONY: verify-lint
verify-lint:
	golangci-lint run

.PHONY: verify-mod-tidy
verify-mod-tidy:
	PASSES="mod_tidy" ./scripts/test.sh


.PHONY: test
test:
	go test ./...

