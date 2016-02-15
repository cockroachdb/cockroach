#!/usr/bin/env sh

set -eu
go get github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing.
xgo --targets=darwin-10.9/amd64 --go=1.6rc2 --ldflags="$($(dirname "${0}")/ldflags.sh)" ${GOPATH%%:*}/src/github.com/cockroachdb/cockroach
