#!/usr/bin/env bash
set -exuo pipefail

# This is intended to run in a CI to ensure dependencies are properly vendored.

echo "checking that 'vendor' matches manifest"
rm -rf vendor/*/*
gvt restore 2>&1
! git -C vendor status --porcelain | read || (git -C vendor status; git -C vendor diff -a; exit 1)

echo "checking that all deps are in 'vendor''"
missing=$(go list -f '{{ join .XTestImports "\n"}}{{"\n"}}{{ join .TestImports "\n"}}{{"\n"}}{{ join .Deps "\n"}}' . ./pkg/... \
   | grep -v "^github.com/cockroachdb/cockroach" \
   | sort | uniq \
   | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}')

if [ -n "$missing" ]; then
  echo "vendor is missing some 3rd-party dependencies:"
  echo "$missing"
  exit 1
fi
