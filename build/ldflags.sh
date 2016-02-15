#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")/.."

echo '-X "github.com/cockroachdb/cockroach/util.buildTag='$(git describe --dirty --tags)'"' \
     '-X "github.com/cockroachdb/cockroach/util.buildTime='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "github.com/cockroachdb/cockroach/util.buildDeps='$(build/depvers.sh)'"'
