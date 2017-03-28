#!/usr/bin/env bash
set -exuo pipefail

# This is intended to run in a CI to ensure dependencies are properly vendored.

echo "installing desired glide version"
go get -d github.com/Masterminds/glide
# ab0972eb merged our fix for correctly vendoring submodule contents.
git -C $GOPATH/src/github.com/Masterminds/glide fetch
git -C $GOPATH/src/github.com/Masterminds/glide checkout ab0972eb
go install github.com/Masterminds/glide

echo "checking that 'vendor' matches manifest"
glide install
! git -C vendor status --porcelain | read || (git -C vendor status; git -C vendor diff -a 1>&2; exit 1)

echo "checking that all deps are in 'vendor''"

top_deps=$(go list -f '{{join .Imports "\n"}}{{"\n"}}{{join .TestImports "\n"}}{{"\n"}}{{join .XTestImports "\n"}}' ./pkg/... | \
    sort -u | grep -v '^C$')
cmd_deps=$(sed -n 's,[[:space:]]*_[[:space:]]*"\(.*\)",./vendor/\1,p' build/tool_imports.go)

deps="
$top_deps
$cmd_deps
"

# Note that grep's exit status is ignored here to allow for packages with no
# dependencies.
missing=$(echo "$deps" | \
	xargs go list -f '{{if not .Standard}}{{join .Deps "\n" }}{{end}}' | \
	sort -u | grep -v '^C$' | \
	xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' | \
	grep -v '^github.com/cockroachdb/cockroach' || true)

if [ -n "$missing" ]; then
  echo "vendor is missing some 3rd-party dependencies:"
  echo "$missing"
  exit 1
fi

echo "all 3rd-party dependencies appear to be properly vendored."
