#!/bin/bash

set -eo pipefail

source "$(dirname "$0")/teamcity-support.sh"

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# Create an SSH key if we don't have one or roachprod's AWS client library
# complains.
if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  run ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

artifacts=$PWD/artifacts/$(date +"%Y%m%d")-${TC_BUILD_ID}
mkdir -p "$artifacts"

if_tc tc_start_block "Compile roachprod"
run go get -u -v github.com/cockroachdb/roachprod
run git -C "$(go env GOPATH)/src/github.com/cockroachdb/roachprod" rev-parse HEAD
if_tc tc_end_block "Install roachprod"

if_tc tc_start_block "Compile CockroachDB"
run make build TYPE=release-linux-gnu
# Use this instead of the `make build` above for faster debugging iteration.
# run curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach-linux-2.6.32-gnu-amd64
# run chmod +x cockroach-linux-2.6.32-gnu-amd64
if_tc tc_end_block "Compile CockroachDB"

if_tc tc_start_block "Compile Workload"
run make bin/roachtest bin/workload
if_tc tc_end_block "Compile Workload"

if_tc tc_start_block "Run roachtest"
run bin/roachtest run \
  --cluster-id "${TC_BUILD_ID}" \
  --slack-token "${SLACK_TOKEN}" \
  --cockroach "$PWD/cockroach-linux-2.6.32-gnu-amd64" \
  --workload "$PWD/bin/workload" \
  --artifacts "$artifacts"
if_tc tc_end_block "Run roachtest"

if_tc tc_start_block "Upload artifacts"
# Only upload artifacts if in TeamCity.
if_tc run gsutil -m -h "Content-Type: text/plain" cp -r \
    "$artifacts" "gs://cockroach-acceptance-results/teamcity-nightly/$TC_BUILD_ID"
if_tc tc_end_block "Upload artifacts"
