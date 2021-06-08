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

make bin/roachprod bin/roachtest

rm -fr vendor/github.com/cockroachdb/pebble
git clone https://github.com/cockroachdb/pebble vendor/github.com/cockroachdb/pebble
pushd vendor/github.com/cockroachdb/pebble
GOOS=linux go build -v -mod=vendor -o pebble.linux ./cmd/pebble
popd
mv vendor/github.com/cockroachdb/pebble/pebble.linux .
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

# Each roachtest's artifacts are zip'd. Unzip them all and remove the .zips.
find $artifacts -name '*.zip' -execdir unzip {} \;
find $artifacts -name '*.zip' -execdir rm {} \;

# mkbench expects artifacts to be gzip compressed.
find $artifacts -name '*.log' | xargs gzip -9

# mkbench expects the benchmark data to be stored in data/YYYYMMDD.
mkdir data
ln -sf $PWD/artifacts data/$(date +"%Y%m%d")

go build -o mkbench github.com/cockroachdb/pebble/internal/mkbench
aws s3 cp s3://pebble-benchmarks/data.js data.js
./mkbench

aws s3 cp data.js s3://pebble-benchmarks/data.js
aws s3 sync --exclude "*/_runner-logs/*"  data/ s3://pebble-benchmarks/data/

exit "$exit_status"
