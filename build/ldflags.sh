#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")/.."

echo '-X "github.com/cockroachdb/cockroach/build.tag='$(git describe --dirty --tags)'"' \
     '-X "github.com/cockroachdb/cockroach/build.time='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "github.com/cockroachdb/cockroach/build.deps='$(build/depvers.sh)'"'
