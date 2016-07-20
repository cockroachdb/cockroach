if [ -n "${CIRCLE_ARTIFACTS-}" ]; then
  outdir="${CIRCLE_ARTIFACTS}"
elif [ -n "${TMPDIR-}" ]; then
  outdir="${TMPDIR}"
else
  outdir="/tmp"
fi

coverage_dir="${outdir}/coverage"
coverage_profile="${coverage_dir}/coverage.out"
coverage_mode=set

rm -rf "$coverage_dir"
mkdir -p "$coverage_dir"

# Run "make coverage" on each package.
for pkg in $(go list ./...); do
    f="${coverage_dir}/$(echo $pkg | tr / -).cover"
    touch $f
    time ${builder} make coverage \
        PKG="$pkg" \
        TESTFLAGS="-v -coverprofile=$f -covermode=$coverage_mode" | \
        tee "${outdir}/coverage.log"
done

# Merge coverage profiles.
echo "mode: $coverage_mode" > "$coverage_profile"
grep -h -v "^mode:" "$coverage_dir"/*.cover >> "$coverage_profile"

# Upload profiles to Coveralls.
goveralls \
    -coverprofile="$coverage_profile" \
    -ignore='*/*.pb.go,*/*/*.pb.go' \
    -service=teamcity \
    -repotoken=$COVERALLS_TOKEN
