#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")/.."

# The build.utcTime format must remain in sync with TimeFormat in info.go.
echo '-X "github.com/cockroachdb/cockroach/build.tag='$(git describe --dirty --tags)'"' \
     '-X "github.com/cockroachdb/cockroach/build.utcTime='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "github.com/cockroachdb/cockroach/build.deps='$(build/depvers.sh)'"'
