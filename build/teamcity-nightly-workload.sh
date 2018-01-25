#!/bin/bash

set -ex

go get -u github.com/cockroachlabs/roachprod
go install github.com/cockroachlabs/roachprod
go get -u github.com/danhhz/roachperf
go install github.com/danhhz/roachperf
make $PWD/bin/workload

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
tar -zxf google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
yes "" | ./google-cloud-sdk/install.sh
source google-cloud-sdk/path.bash.inc

(set +x; echo $GOOGLE_CREDENTIALS > creds.json)
gcloud auth activate-service-account --key-file=creds.json
eval $(ssh-agent)

mkdir -p archive
cd archive
cp ../bin/workload ../cockroach .

./workload teamcity-nightlies ${TC_BUILD_ID}

cp -R workload-test* ../artifacts
roachperf upload workload-test* || true
