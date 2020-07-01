#!/usr/bin/env bash
#
# This script is run by the Pebble Nightly - AWS TeamCity build
# configuration.

set -eo pipefail

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

set -ux

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

# Disable global -json flag.
export PATH=$PATH:$(GOFLAGS=; go env GOPATH)/bin

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)

make bin/roachprod bin/roachtest > "${artifacts}/build.txt" 2>&1 || cat "${artifacts}/build.txt"

rm -fr vendor/github.com/cockroachdb/pebble
git clone https://github.com/cockroachdb/pebble vendor/github.com/cockroachdb/pebble
GOOS=linux go build -o pebble.linux github.com/cockroachdb/pebble/cmd/pebble
export PEBBLE_BIN=pebble.linux

# NB: We specify "true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
  --build-tag "${build_tag}" \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "aws" \
  --cockroach "true" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "true" \
  --artifacts "$artifacts" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  tag:pebble pebble; then
  exit_status=$?
fi

# TODO(peter):
# - Collect the artifacts. Add them to a branch of the Pebble repo.
# - Process the artifacts into a static site served by Github pages.

exit "$exit_status"
