#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

go get -u github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing.
# If changing the OS/arch target, adjust the filename in push-aws.sh.
make xgo-build GOFLAGS='--image cockroachdb/xgo:20170218 --targets=darwin-10.9/amd64' TYPE=release
