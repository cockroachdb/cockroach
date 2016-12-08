#!/usr/bin/env bash
set -exuo pipefail

# This is intended to run in a CI to ensure dependencies are properly vendored.

echo "installing desired glide version"
go get github.com/Masterminds/glide
git -C $GOPATH/src/github.com/Masterminds/glide checkout v0.12.3
go install github.com/Masterminds/glide

echo "checking that 'vendor' matches manifest"
./scripts/glide.sh install
! git -C vendor status --porcelain | read || (git -C vendor status; git -C vendor diff -a 1>&2; exit 1)

echo "checking that all deps are in 'vendor''"

missing=$(sed -n 's,[[:space:]]*_[[:space:]]*"\(.*\)",\1,p' build/tool_imports.go | awk '{print} END {print "./pkg/..."}' \
  | xargs go list -f '{{ join .Deps "\n"}}{{"\n"}}{{ join .TestImports "\n"}}{{"\n"}}{{ join .XTestImports "\n"}}' \
  | grep -v '^github.com/cockroachdb/cockroach' \
  | sort | uniq \
  | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}')

if [ -n "$missing" ]; then
  echo "vendor is missing some 3rd-party dependencies:"
  echo "$missing"
  exit 1
fi
