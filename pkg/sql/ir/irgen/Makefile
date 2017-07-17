# Trigger these rules by running `make generate PKG=./pkg/sql/ir/irgen` from the
# repository root to ensure your PATH includes vendored binaries.

SHELL = /usr/bin/env bash

all: test/d/example.ir.go

test/d/example.ir.go: d/template.go test/example.def
	go build
	./irgen $^ | gofmt >$@
