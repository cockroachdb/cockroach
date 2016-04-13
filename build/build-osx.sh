#!/usr/bin/env sh

set -eu

go get github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing.
# If changing the OS/arch target, adjust the filename in push-aws.sh.
xgo --image cockroachdb/xgo:20160413 --targets=darwin-10.9/amd64 --go=1.6.x --ldflags="$($(dirname "${0}")/ldflags.sh)" ${GOPATH%%:*}/src/github.com/cockroachdb/cockroach
