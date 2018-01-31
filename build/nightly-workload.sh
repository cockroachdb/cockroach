#!/bin/bash

set -ex

# Work around a roachprod bug by creating the hosts directory.
# TODO(benesch): Fix roachprod.
mkdir -p ~/.roachprod/hosts

# This `set +x`/paren magic turns off the `set -x` so we don't leak secrets.
# (set +x; echo $GOOGLE_CREDENTIALS > creds.json)
# gcloud auth activate-service-account --key-file=creds.json

go get -u -v github.com/cockroachdb/roachprod

# make build TYPE=release-linux-gnu
# Use this instead of make build for faster debugging iteration.
curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
chmod +x cockroach-linux-2.6.32-gnu-amd64

make bin/workload

go test -v -really -clusterid "${TC_BUILD_ID}" -cockroach "$PWD/cockroach" -workload "$PWD/workload" ./pkg/nightly

# Upload artifacts. Currently broken because of s3 auth. Copied from the
# "Roachperf Nightly" build, where it is also currently broken.
cp -R workload-test* artifacts/
roachprod upload workload-test* || true
