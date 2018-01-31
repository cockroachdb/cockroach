#!/bin/bash

set -ex

go get -u github.com/cockroachdb/roachprod

make bin/workload

# TODO(benesch): Make roachprod deal with this.
cat > ~/.ssh/config <<EOF
Host *
    StrictHostKeyChecking no
EOF

# This `set +x`/paren magic turns off the `set -x` so we don't leak secrets.
# (set +x; echo $GOOGLE_CREDENTIALS > creds.json)
gcloud auth activate-service-account --key-file=creds.json

# nightlies assumes cockroach and workload are in the CWD.
cp cockroach-linux-2.6.32-gnu-amd64 cockroach
cp bin/workload .

# Work around a roachprod bug by creating the hosts directory.
mkdir -p ~/.roachprod/hosts

# teamcity-nightlies puts the arg we give into the name of every cluster it
# creates, so this should let us match up clusters to invocations of this build.
export COCKROACH_NIGHTLY_CLUSTER_ID=${TC_BUILD_ID:-}
go test -v -tags nightly ./pkg/nightly

# Upload artifacts. Currently broken because of s3 auth. Copied from the
# "Roachperf Nightly" build, where it is also currently broken.
cp -R workload-test* artifacts/
roachprod upload workload-test* || true
