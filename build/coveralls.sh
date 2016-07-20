if [ -z "$COVERALLS_TOKEN" ]; then
  echo "FAIL: Missing or empty COVERALLS_TOKEN."
  exit 1
fi
if [ -n "${TMPDIR-}" ]; then
  outdir="${TMPDIR}"
else
  outdir="/tmp"
fi

coverage_dir="${outdir}/coverage"
coverage_profile="${coverage_dir}/coverage.out"
coverage_mode=count

rm -rf "$coverage_dir"
mkdir -p "$coverage_dir"

packages=$(go list ./...)
coverpkg=$(go list ./... | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/,/g')

# Run "make coverage" on each package.
for pkg in $(go list ./...); do
    if [ -z "$(go list -f '{{join .TestGoFiles ""}}{{join .XTestGoFiles ""}}' $pkg)" ]; then
      echo "$pkg: Skipping due to no test files."
      continue
    fi
    # Find all cockroach packages that are imported by this package or in it's
    # tests.
    f="${coverage_dir}/$(echo $pkg | tr / -).cover"
    touch $f
    time ${builder} make coverage \
        PKG="$pkg" \
        TESTFLAGS="-v -coverprofile=$f -covermode=$coverage_mode -coverpkg=$coverpkg" | \
        tee "${outdir}/coverage.log"
done

# Merge coverage profiles.
gocovmerge "$coverage_dir"/*.cover > "$coverage_profile"

# Upload profiles to Coveralls.
goveralls \
    -coverprofile="$coverage_profile" \
    -ignore='*/*.pb.go,*/*/*.pb.go,*/*/*/*.pb.go' \
    -service=teamcity \
    -repotoken=$COVERALLS_TOKEN
