#!/usr/bin/env bash

set -euxo pipefail

if [ -z "${COVERALLS_TOKEN-}" ]; then
  echo "FAIL: Missing or empty COVERALLS_TOKEN."
  exit 1
fi
if [ -z "${CODECOV_TOKEN-}" ]; then
  echo "FAIL: Missing or empty CODECOV_TOKEN."
  exit 1
fi

prefix=github.com/cockroachdb/cockroach/pkg

coverage_dir=coverage
coverage_profile=$coverage_dir/coverage.out

trap "rm -rf $coverage_dir" EXIT

rm -rf $coverage_dir
mkdir -p $coverage_dir

# Run coverage on each package, because go test "cannot use test profile flag
# with multiple packages". See https://github.com/golang/go/issues/6909.
for pkg in $(go list $prefix/...); do
  # Only generate coverage for cockroach dependencies.
  #
  # Note that grep's exit status is ignored here to allow for packages with no
  # dependencies.
  coverpkg=$(go list -f '{{join .Imports "\n"}}{{"\n"}}{{join .TestImports "\n"}}{{"\n"}}{{join .XTestImports "\n"}}' $pkg | \
    sort -u | grep -v '^C$' | \
    xargs go list -f '{{if not .Standard}}{{join .Deps "\n" }}{{end}}' | \
    sort -u | grep -v '^C$' | \
    xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' | \
    grep $prefix || true)

  time make test PKG="$pkg" TESTFLAGS="-coverpkg=${coverpkg//$'\n'/,} -coverprofile=${coverage_dir}/${pkg//\//-}.cover -covermode=count"
done

# Merge coverage profiles and remove lines that match our ignore filter.
gocovmerge $coverage_dir/*.cover | \
  grep -vE "$prefix/(acceptance|cmd|ui/embedded|bench|.*\.pb(\.gw)?\.go)" > $coverage_profile

# Upload profiles to coveralls.io.
goveralls \
  -coverprofile=$coverage_profile \
  -service=teamcity \
  -repotoken="$COVERALLS_TOKEN"

# Upload profiles to codecov.io. Uses CODECOV_TOKEN.
bash <(curl -s https://codecov.io/bash) -f $coverage_profile
