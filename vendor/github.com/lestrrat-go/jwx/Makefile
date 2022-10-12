.PHONY: generate realclean cover viewcover test lint check_diffs imports tidy
generate: 
	@go generate
	@$(MAKE) generate-jwa generate-jwe generate-jwk generate-jws generate-jwt

generate-%:
	@echo "> Generating for $(patsubst generate-%,%,$@)"
	@go generate $(shell pwd -P)/$(patsubst generate-%,%,$@)

realclean:
	rm coverage.out

test-cmd:
	go test -v -race $(TESTOPTS)

test:
	$(MAKE) TESTOPTS=./... test-cmd
	$(MAKE) -f $(PWD)/Makefile -C examples test-cmd
	$(MAKE) -f $(PWD)/Makefile -C bench/performance test-cmd

cover-cmd:
	$(MAKE) test-cmd 
	$(MAKE) -f $(PWD)/Makefile -C examples TESTOPTS= test-cmd
	$(MAKE) -f $(PWD)/Makefile -C bench/performance TESTOPTS= test-cmd
	$(MAKE) -f $(PWD)/Makefile -C cmd/jwx TESTOPTS= test-cmd
	@# This is NOT cheating. tools to generate code, and tools to
	@# run tests don't need to be included in the final result.
	@cat coverage.out.tmp | grep -v "internal/jose" | grep -v "internal/jwxtest" | grep -v "internal/cmd" > coverage.out
	@rm coverage.out.tmp

cover:
	$(MAKE) cover-stdlib

cover-stdlib:
	$(MAKE) cover-cmd TESTOPTS="-coverpkg=./... -coverprofile=coverage.out.tmp ./..."

cover-goccy:
	$(MAKE) cover-cmd TESTOPTS="-tags jwx_goccy -coverpkg=./... -coverprofile=coverage.out.tmp ./..."

cover-es256k:
	$(MAKE) cover-cmd TESTOPTS="-tags jwx_es256k -coverpkg=./... -coverprofile=coverage.out.tmp ./..."

cover-all:
	$(MAKE) cover-cmd TESTOPTS="-tags jwx_goccy,jwx_es256k -coverpkg=./... -coverprofile=coverage.out.tmp ./..."

smoke-cmd:
	$(MAKE) test-cmd
	$(MAKE) -f $(PWD)/Makefile -C examples test-cmd
	$(MAKE) -f $(PWD)/Makefile -C bench/performance test-cmd
	$(MAKE) -f $(PWD)/Makefile -C cmd/jwx test-cmd

smoke:
	$(MAKE) smoke-stdlib

smoke-stdlib:
	$(MAKE) smoke-cmd TESTOPTS="-short ./..."

smoke-goccy:
	$(MAKE) smoke-cmd TESTOPTS="-short -tags jwx_goccy ./..."

smoke-es256k:
	$(MAKE) smoke-cmd TESTOPTS="-short -tags jwx_es256k ./..."

smoke-all:
	$(MAKE) smoke-cmd TESTOPTS="-short -tags jwx_goccy,jwx_es256k ./..."

viewcover:
	go tool cover -html=coverage.out

lint:
	golangci-lint run ./...

check_diffs:
	./scripts/check-diff.sh

imports:
	goimports -w ./

tidy:
	./scripts/tidy.sh

