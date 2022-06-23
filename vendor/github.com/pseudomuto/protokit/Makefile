.PHONY: bench release setup test

VERSION = $(shell cat version.go | sed -n 's/.*const Version = "\(.*\)"/\1/p')

setup:
	$(info Synching dev tools and dependencies...)
	@if test -z $(which retool); then go get github.com/twitchtv/retool; fi
	@retool sync
	@retool do dep ensure

fixtures/fileset.pb: fixtures/*.proto
	$(info Generating fixtures...)
	@cd fixtures && go generate

bench:
	go test -bench=.

test: fixtures/fileset.pb
	@go test -race -cover ./ ./utils

test-ci: fixtures/fileset.pb bench
	@retool do goverage -race -coverprofile=coverage.txt -covermode=atomic ./ ./utils

release:
	@echo Releasing v${VERSION}...
	git add CHANGELOG.md version.go
	git commit -m "Bump version to v${VERSION}"
	git tag -m "Version ${VERSION}" "v${VERSION}"
	git push && git push --tags
