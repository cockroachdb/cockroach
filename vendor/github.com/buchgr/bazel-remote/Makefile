# go source files, ignore vendor directory
SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

.PHONY: gofmt

gofmt:
	@gofmt -l -w $(SRC)
