#!/usr/bin/env bash
set -exuo pipefail

echo "checking that 'vendor' matches manifest"
rm -rf vendor/*/*
gvt restore 2>&1
! git -C vendor status --porcelain | read || (git -C vendor status; git -C vendor diff -a; exit 1)

missing=$(go list -f '{{ join .Deps "\n"}}' . | grep -v "^github.com/cockroachdb/cockroach" | xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}')

if [ -n "$missing" ]; then
  echo "vendor is missing some 3rd-party dependencies:"
  echo "$missing"
  exit 1
fi
