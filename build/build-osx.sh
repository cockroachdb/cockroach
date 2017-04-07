#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

build/builder.sh make build TYPE=release-darwin
