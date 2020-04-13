#!/usr/bin/env bash
set -euo pipefail

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


# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "$artifacts"
chmod o+rwx "${artifacts}"

# We do, however, want to write the stats files with datestamps before uploading
stats_artifacts="${artifacts}"/$(date +"%%Y%%m%%d")-${TC_BUILD_ID}
mkdir -p "${stats_artifacts}"
chmod o+rwx "${stats_artifacts}"

export PATH=$PATH:$(go env GOPATH)/bin

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)
git checkout master
git pull origin master

git rev-parse HEAD
make bin/workload bin/roachtest bin/roachprod > "${artifacts}/build.txt" 2>&1 || cat "${artifacts}/build.txt"

# release-2.0 names the cockroach binary differently.
if [[ -f cockroach-linux-2.6.32-gnu-amd64 ]]; then
  mv cockroach-linux-2.6.32-gnu-amd64 cockroach.linux-2.6.32-gnu-amd64
fi

chmod +x cockroach.linux-2.6.32-gnu-amd64


# NB: Teamcity has a 1300 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 1200 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
exit_status=0
if ! timeout -s INT $((1200*60)) bin/roachtest run \
  --count="${COUNT-1}" \
  --debug="${DEBUG-false}" \
  --build-tag="${build_tag}" \
  --slack-token="${SLACK_TOKEN}" \
  --cluster-id="${TC_BUILD_ID}" \
  --zones="us-central1-b,us-west1-b,europe-west2-b" \
  --cockroach="$PWD/cockroach.linux-2.6.32-gnu-amd64" \
  --roachprod="$PWD/bin/roachprod" \
  --workload="$PWD/bin/workload" \
  --artifacts="$stats_artifacts" \
  --parallelism=16 \
  --cpu-quota=1024 \
  --teamcity=true \
  "${TESTS}"; then
  exit_status=$?
fi

# Upload any stats.json files to the cockroach-nightly bucket.
if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
    for file in $(find ${artifacts#${PWD}/} -name stats.json); do
        gsutil cp ${file} gs://cockroach-nightly/${file}
    done
fi

exit "$exit_status"
