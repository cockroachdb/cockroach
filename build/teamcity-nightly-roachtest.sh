#!/usr/bin/env bash
# Entry point for the nightly roachtests. These are run from CI and require
# appropriate secrets for the ${CLOUD} parameter (along with other things,
# apologies, you're going to have to dig around for them below or even better
# yet, look at the job).
t
# Note that when this script is called, the cockroach binary to be tested
# already exists in the current directory.

set -euo pipefail

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

make bin/workload bin/roachtest bin/roachprod > "${artifacts}/build.txt" 2>&1 || cat "${artifacts}/build.txt"

# release-2.0 names the cockroach binary differently.
if [[ -f cockroach-linux-2.6.32-gnu-amd64 ]]; then
  mv cockroach-linux-2.6.32-gnu-amd64 cockroach.linux-2.6.32-gnu-amd64
fi
chmod +x cockroach.linux-2.6.32-gnu-amd64

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

. "build/teamcity-nightly-roachtest-${CLOUD}.sh"
