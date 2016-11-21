#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

# The build.utcTime format must remain in sync with TimeFormat in info.go.
echo '-X "github.com/cockroachdb/cockroach/pkg/build.tag='$(git describe --tags --exact-match 2> /dev/null || git rev-parse --short HEAD)'"' \
     '-X "github.com/cockroachdb/cockroach/pkg/build.utcTime='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "github.com/cockroachdb/cockroach/pkg/build.deps='$(build/depvers.sh)'"'
