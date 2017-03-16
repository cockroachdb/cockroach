#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

go get -u github.com/karalabe/xgo
# OSX 10.9 is the most recent version available at time of writing. If changing
# the OS/arch target, adjust the filename in push-aws.sh.
#
# We additionally set GOVERS= to disable the Go version check. This allows
# building in environments without a Go toolchain, like TeamCity build agents.
# xgo will use the Go toolchain in its Docker image regardless.
make xgo-build GOVERS= GOFLAGS='--image cockroachdb/xgo:20170218 --targets=darwin-10.9/amd64' TYPE=release
