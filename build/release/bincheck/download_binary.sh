#!/usr/bin/env bash

set -euo pipefail

binary_suffix=$1
binary_source=""
cockroach_version=""
while read line; do
  # Trailing \r on Windows sneaks into the variables, let's nuke it
  line=$(echo $line | tr -d '\r')
  case "$line" in
  \#*|"") continue ;;
  test:*)
    binary_source="https://binaries-test.cockroachdb.com"
    parts=(${line//:/ })
    export COCKROACH_VERSION=${parts[1]}
    ;;
  v*)
    binary_source="https://binaries.cockroachdb.com"
    export COCKROACH_VERSION=$line
    ;;
  *)
    export COCKROACH_SHA=$line
  esac
done < VERSION

binary_url="${binary_source}/cockroach-$COCKROACH_VERSION.${binary_suffix}"

mkdir -p mnt

# Check if this is a tarball or zip.
if [[ "${binary_suffix}" == *.tgz ]]; then
  curl -sSfL "${binary_url}" > cockroach.tar.gz
  tar zxf cockroach.tar.gz -C mnt --strip-components=1
else
  curl -sSfL "${binary_url}" > cockroach.zip
  7z e -omnt cockroach.zip
fi

echo "Downloaded ${binary_url}"
