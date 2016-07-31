#!/usr/bin/env sh

set -eu

go get -u github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing.
# If changing the OS/arch target, adjust the filename in push-aws.sh.
# TODO(tamird): update to go1.6.3 or go1.7 when
# https://github.com/karalabe/xgo/pull/60/files is merged.
xgo --image cockroachdb/xgo:20160526 --targets=darwin-10.9/amd64 --go=1.6.2 --ldflags="$($(dirname "${0}")/ldflags.sh)" ${GOPATH%%:*}/src/github.com/cockroachdb/cockroach
