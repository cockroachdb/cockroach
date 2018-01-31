#!/bin/bash

set -ex

# Below assumes TC_BUILD_ID, so if it's unset, fill it in with something.
TC_BUILD_ID="${TC_BUILD_ID:-nobuild-`date +%Y-%m-%d-%H%M%S`}"

go get -u github.com/cockroachdb/roachprod

# teamcity-nightlies assumes cockroach and workload are the cwd.
make build TYPE=release-linux-gnu
# Use this instead of make build for faster debugging iteration.
# curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
# chmod +x cockroach-linux-2.6.32-gnu-amd64
cp cockroach-linux-2.6.32-gnu-amd64 cockroach
make bin/workload
cp bin/workload .

# This `set +x`/paren magic turns off the `set -x` so we don't leak secrets.
(set +x; echo $GOOGLE_EPHEMERAL_CREDENTIALS > creds_ephemeral.json)
gcloud auth activate-service-account --key-file=creds_ephemeral.json

# teamcity-nightlies puts the arg we give into the name of every cluster it
# creates, so this should let us match up clusters to invocations of this build.
mkdir -p ~/.roachprod/hosts
./workload teamcity-nightlies teamcity-nightly-workload-${TC_BUILD_ID}
mkdir -p artifacts
cp -R workload-test*/* artifacts/
gsutil -h "Content-Type:text/plain" cp \
    -R workload-test*/* gs://cockroach-acceptance-results/teamcity-nightly/${TC_BUILD_ID}/
