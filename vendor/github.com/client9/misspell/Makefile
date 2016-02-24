
all: install lint test

install:
	go get -t ./...
	go build ./...
	go run cmd/genwords/*.go > ./words.go
	go install ./cmd/misspell

lint:
	golint ./...
	go vet ./...
	find . -name '*.go' | xargs gofmt -w -s

test: install
	go test .
	misspell *.md replace.go cmd/misspell/*.go

clean:
	rm -f *~
	go clean ./...
	git gc

ci: install lint test

docker-ci:
	docker run --rm \
		-e COVERALLS_REPO_TOKEN=$COVERALLS_REPO_TOKEN \
		-v $(PWD):/go/src/github.com/client9/misspell \
		-w /go/src/github.com/client9/misspell \
		nickg/golang-dev-docker \
		make ci

.PHONY: ci docker-ci
