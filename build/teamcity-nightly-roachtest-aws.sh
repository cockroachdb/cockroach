#!/usr/bin/env bash
set -euo pipefail

# TODO(peter,benesch): This script contains a ton of duplication with Roachtest Nightly - GCE

# Note that when this script is called, the cockroach binary to be tested
# already exists in the current directory.

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

set -x

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

export PATH=$PATH:$(go env GOPATH)/bin

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)
git checkout master
git pull origin master

git rev-parse HEAD
make bin/workload bin/roachtest bin/roachprod

# release-2.0 names the cockroach binary differently.
if [[ -f cockroach-linux-2.6.32-gnu-amd64 ]]; then
  mv cockroach-linux-2.6.32-gnu-amd64 cockroach.linux-2.6.32-gnu-amd64
fi

chmod +x cockroach.linux-2.6.32-gnu-amd64

# Artifacts dir needs to match up with TC's (without any extra components).
artifacts=$PWD/artifacts
mkdir -p "$artifacts"

# We do, however, want to write the stats files with datestamps before uploading
stats_artifacts="${artifacts}"/$(date +"%%Y%%m%%d")-${TC_BUILD_ID}
mkdir -p "${stats_artifacts}"
chmod o+rwx "${stats_artifacts}"


# NB: Teamcity has a 1080 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 1000 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b 
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
  --build-tag "${build_tag}" \
  --slack-token "${SLACK_TOKEN}" \
  --cluster-id "${TC_BUILD_ID}" \
  --cloud "aws" \
  --cockroach "$PWD/cockroach.linux-2.6.32-gnu-amd64" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "$PWD/bin/workload" \
  --artifacts "$stats_artifacts" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  "kv(0|95)|ycsb|tpcc/(headroom/n4cpu16)|tpccbench/(nodes=3/cpu=16)"; then
  exit_status=$?
fi

# Upload any stats.json files to the cockroach-nightly bucket on master.
if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
    for file in $(find ${artifacts#${PWD}/} -name stats.json); do
        gsutil cp ${file} gs://cockroach-nightly-aws/${file}
    done
fi

exit "$exit_status"
