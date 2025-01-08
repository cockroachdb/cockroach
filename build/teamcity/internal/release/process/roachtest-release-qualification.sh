#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'error: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  exit 1
fi

set -x

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

artifacts=$PWD/artifacts/$(date +"%Y%m%d")-${TC_BUILD_ID}
mkdir -p "$artifacts"

if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
  tarball_platform="linux-amd64-fips"
  fips_flag="--metamorphic-fips-probability 1"
else
  tarball_platform="linux-amd64"
  fips_flag=""
fi

release_version=$(echo $TC_BUILD_BRANCH | sed -e 's/provisional_[[:digit:]]*_//')
curl -f -s -S -o- "https://storage.googleapis.com/cockroach-builds-artifacts-prod/cockroach-${release_version}.${tarball_platform}.tgz" | tar ixfz - --strip-components 1
chmod +x cockroach

run_bazel <<'EOF'
bazel build --config crosslinux //pkg/cmd/workload //pkg/cmd/roachtest //pkg/cmd/roachprod
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
mkdir -p bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
cp $BAZEL_BIN/pkg/cmd/workload/workload_/workload    bin
chmod a+w bin/roachprod bin/roachtest bin/workload
EOF

# NB: Teamcity has a 7920 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 7800 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
timeout -s INT $((7800*60)) bin/roachtest run \
  --suite release_qualification \
  --cluster-id "${TC_BUILD_ID}" \
  --zones "us-central1-b,us-west1-b,europe-west2-b" \
  --cockroach "$PWD/cockroach" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "$PWD/bin/workload" \
  --artifacts "$artifacts" \
  --parallelism 5 \
  $fips_flag \
  --teamcity
