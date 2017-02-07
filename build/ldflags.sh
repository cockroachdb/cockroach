#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

build_version="$(git describe --tags --exact-match 2> /dev/null || git rev-parse --short HEAD)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  build_version="${build_version}-dirty"
fi

# The build.utcTime format must remain in sync with TimeFormat in info.go.
echo '-X "github.com/cockroachdb/cockroach/pkg/build.tag='${build_version}'"' \
     '-X "github.com/cockroachdb/cockroach/pkg/build.utcTime='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "github.com/cockroachdb/cockroach/pkg/build.rev='$(git rev-parse HEAD)'"'
