#!/usr/bin/env sh

set -eu

go get -u github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing.
# If changing the OS/arch target, adjust the filename in push-aws.sh.
xgo --image cockroachdb/xgo:20161024 --targets=darwin-10.9/amd64 --go=1.7.3 --ldflags="$("$(dirname "${0}")"/ldflags.sh)" "${GOPATH%%:*}"/src/github.com/cockroachdb/cockroach
