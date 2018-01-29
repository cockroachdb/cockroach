#!/bin/bash

set -ex

go get -u github.com/cockroachdb/roachprod

make bin/workload

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
tar -zxf google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
yes "" | ./google-cloud-sdk/install.sh
source google-cloud-sdk/path.bash.inc

# This `set +x`/paren magic turns off the `set -x` so we don't leak secrets.
(set +x; echo $GOOGLE_CREDENTIALS > creds.json)
gcloud auth activate-service-account --key-file=creds.json
eval $(ssh-agent)
ssh-keygen -f ~/.ssh/google_compute_engine -N ''
ssh-add ~/.ssh/google_compute_engine

# teamcity-nightlies assumes cockroach and workload are the cwd.
cp cockroach-linux-2.6.32-gnu-amd64 cockroach
cp bin/workload .

# teamcity-nightlies puts the arg we give into the name of every cluster it
# creates, so this should let us match up clusters to invocations of this build.
./workload teamcity-nightlies teamcity-nightly-workload-${TC_BUILD_ID}
cp -R workload-test* artifacts/

# Currently broken because of s3 auth. Copied from the "Roachperf Nightly"
# build, where it is also currently broken.
roachprod upload workload-test* || true
