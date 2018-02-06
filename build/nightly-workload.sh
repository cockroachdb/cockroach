#!/bin/bash

set -eo pipefail

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# IMPORTANT: To avoid leaking credentials, we don't set -x until after
# processing credentials.
set -x

# If TC_BUILD_ID is unset, as it likely is locally, we simply write artifacts
# directly into the artifacts directory.
artifacts=$PWD/artifacts/${TC_BUILD_ID}
mkdir -p "$artifacts"

go get -u -v github.com/cockroachdb/roachprod

make build TYPE=release-linux-gnu

# Use this instead of the `make build` above for faster debugging iteration.
# curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
# chmod +x cockroach-linux-2.6.32-gnu-amd64

make bin/workload

testflags=(
  -v -slow
  -clusterid "${TC_BUILD_ID}"
  -cockroach "$PWD/cockroach-linux-2.6.32-gnu-amd64"
  -workload "$PWD/bin/workload"
  -artifacts "$artifacts"
)
make test \
  PKG=./pkg/nightly \
  TESTFLAGS="$(printf "%q " "${testflags[@]}")" \
  TESTS="${TESTS:-.}" \
  TESTTIMEOUT=12h

# Don't upload test results if not running in TeamCity.
if [[ "$TC_BUILD_ID" ]]; then
  gsutil -m -h "Content-Type: text/plain" cp -r \
    "$artifacts" "gs://cockroach-acceptance-results/teamcity-nightly/$TC_BUILD_ID"
fi
