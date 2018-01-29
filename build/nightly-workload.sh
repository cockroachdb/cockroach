#!/bin/bash

set -ex

go get -u github.com/cockroachdb/roachprod

make bin/workload

# This `set +x`/paren magic turns off the `set -x` so we don't leak secrets.
(set +x; echo $GOOGLE_CREDENTIALS > creds.json)
gcloud auth activate-service-account --key-file=creds.json

# teamcity-nightlies assumes cockroach and workload are the cwd.
cp cockroach-linux-2.6.32-gnu-amd64 cockroach
cp bin/workload .

# teamcity-nightlies puts the arg we give into the name of every cluster it
# creates, so this should let us match up clusters to invocations of this build.
mkdir -p ~/.roachprod/hosts
./workload teamcity-nightlies teamcity-nightly-workload-${TC_BUILD_ID}
cp -R workload-test* artifacts/

# Currently broken because of s3 auth. Copied from the "Roachperf Nightly"
# build, where it is also currently broken.
roachprod upload workload-test* || true
