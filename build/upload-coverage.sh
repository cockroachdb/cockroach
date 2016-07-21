#!/bin/bash

if [ -z "$COVERALLS_TOKEN" ]; then
  echo "FAIL: Missing or empty COVERALLS_TOKEN."
  exit 1
fi
if [ -z "$CODECOV_TOKEN" ]; then
  echo "FAIL: Missing or empty CODECOV_TOKEN."
  exit 1
fi

if [ -n "${TMPDIR-}" ]; then
  outdir="${TMPDIR}"
else
  outdir="/tmp"
fi

main_package="github.com/cockroachdb/cockroach"
# This regex removes files from the uploaded coverage.
ignore_files="$main_package/(acceptance|cmd|ui/embedded|storage/simulation|sql/pgbench|.*\.(pb|pb\.gw)\.go)"

coverage_dir="${outdir}/coverage"
coverage_profile="${coverage_dir}/coverage.out"
coverage_mode=count

rm -rf "$coverage_dir"
mkdir -p "$coverage_dir"

# Run "make coverage" on each package.
for pkg in $(go list ./...); do
  # Verify package has test files.
  if [ -z "$(go list -f '{{join .TestGoFiles ""}}{{join .XTestGoFiles ""}}' $pkg)" ]; then
    echo "$pkg: Skipping due to no test files."
    continue
  fi

  # Only generate coverage for cockroach dependencies. This fetches all test
  # deps and main deps, filters them, and converts them into a comma separated
  # list.
  coverpkg=$(go list  -f '{{join .Deps "\n"}}
{{join .TestImports "\n"}}
{{join .XTestImports "\n"}}' $pkg | \
  sort | uniq | grep "$main_package" | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/,/g')

  # Find all cockroach packages that are imported by this package or in it's
  # tests.
  f="${coverage_dir}/$(echo $pkg | tr / -).cover"
  touch $f
  time ${builder} make coverage \
    PKG="$pkg" \
    TESTFLAGS="-v -coverprofile=$f -covermode=$coverage_mode -coverpkg=$coverpkg" | \
    tee "${outdir}/coverage.log"
done

# Merge coverage profiles and remove lines that match our ignore filter.
gocovmerge "$coverage_dir"/*.cover | grep -vE "$ignore_files" > "$coverage_profile"

# Upload profiles to coveralls.io.
goveralls \
  -coverprofile="$coverage_profile" \
  -service=teamcity \
  -repotoken=$COVERALLS_TOKEN

# Upload profiles to codecov.io.
bash <(curl -s https://codecov.io/bash) -f "$coverage_profile"
