.PHONY: bench test build dist docker examples release lint

export GO111MODULE=on

EXAMPLE_DIR=$(PWD)/examples
DOCS_DIR=$(EXAMPLE_DIR)/doc
PROTOS_DIR=$(EXAMPLE_DIR)/proto

EXAMPLE_CMD=protoc --plugin=protoc-gen-doc \
	-Ithirdparty -Itmp/googleapis -Iexamples/proto \
	--doc_out=examples/doc

DOCKER_CMD=docker run --rm \
	-v $(DOCS_DIR):/out:rw \
	-v $(PROTOS_DIR):/protos:ro \
	-v $(EXAMPLE_DIR)/templates:/templates:ro \
	-v $(PWD)/thirdparty/github.com/mwitkow:/usr/local/include/github.com/mwitkow:ro \
	-v $(PWD)/thirdparty/github.com/envoyproxy:/usr/local/include/github.com/envoyproxy:ro \
	-v $(PWD)/tmp/googleapis/google/api:/usr/local/include/google/api:ro \
	pseudomuto/protoc-gen-doc

VERSION = $(shell cat version.go | sed -n 's/.*const VERSION = "\(.*\)"/\1/p')

resources.go: resources/*.tmpl resources/*.json
	$(info Generating resources...)
	@go run resources/main.go -in resources -out resources.go -pkg gendoc

fixtures/fileset.pb: fixtures/*.proto fixtures/generate.go
	$(info Generating fixtures...)
	@cd fixtures && go generate

tmp/googleapis:
	rm -rf tmp/googleapis tmp/protocolbuffers
	git clone --depth 1 https://github.com/googleapis/googleapis tmp/googleapis
	rm -rf tmp/googleapis/.git
	git clone --depth 1 https://github.com/protocolbuffers/protobuf tmp/protocolbuffers
	cp -r tmp/protocolbuffers/src/* tmp/googleapis/
	rm -rf tmp/protocolbuffers

test: fixtures/fileset.pb resources.go
	@go test -cover -race ./ ./cmd/... ./extensions/...

bench:
	@go test -bench=.

build: resources.go
	@go build ./cmd/...

dist:
	@script/dist.sh

docker:
	@script/push_to_docker.sh

docker_test: build tmp/googleapis docker
	@rm -f examples/doc/*
	@$(DOCKER_CMD) --doc_opt=docbook,example.docbook:Ignore*
	@$(DOCKER_CMD) --doc_opt=html,example.html:Ignore*
	@$(DOCKER_CMD) --doc_opt=json,example.json:Ignore*
	@$(DOCKER_CMD) --doc_opt=markdown,example.md:Ignore*
	@$(DOCKER_CMD) --doc_opt=/templates/asciidoc.tmpl,example.txt:Ignore*

examples: build tmp/googleapis examples/proto/*.proto examples/templates/*.tmpl
	$(info Making examples...)
	@rm -f examples/doc/*
	@$(EXAMPLE_CMD) --doc_opt=docbook,example.docbook:Ignore* examples/proto/*.proto
	@$(EXAMPLE_CMD) --doc_opt=html,example.html:Ignore* examples/proto/*.proto
	@$(EXAMPLE_CMD) --doc_opt=json,example.json:Ignore* examples/proto/*.proto
	@$(EXAMPLE_CMD) --doc_opt=markdown,example.md:Ignore* examples/proto/*.proto
	@$(EXAMPLE_CMD) --doc_opt=examples/templates/asciidoc.tmpl,example.txt:Ignore* examples/proto/*.proto

release:
	@echo Releasing v${VERSION}...
	git add CHANGELOG.md version.go
	git commit -m "Bump version to v${VERSION}"
	git tag -m "Version ${VERSION}" "v${VERSION}"
	git push && git push --tags

lint:
	@which revive >/dev/null || go get github.com/mgechev/revive
	revive --config revive.toml ./...
