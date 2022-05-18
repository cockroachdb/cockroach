#!/usr/bin/env bash

set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

log_into_gcloud
export ROACHPROD_USER=teamcity

set -x

generate_ssh_key

export PATH=$PATH:$(go env GOPATH)/bin

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)
git checkout master
git pull origin master

git rev-parse HEAD

source $root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh

artifacts_subdir=bazel-$(date +"%Y%m%d")-${TC_BUILD_ID}
artifacts=$PWD/artifacts/$artifacts_subdir
mkdir -p "$artifacts"
chmod o+rwx "${artifacts}"

# NB: Teamcity has a 7920 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 7800 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
#
# NB(3): If you make changes here, you should probably make the same change in
# build/teamcity-weekly-roachtest.sh
exit_status=0
timeout -s INT $((7800*60)) bin/roachtest run \
  tag:weekly \
  --build-tag "${build_tag}" \
  --cluster-id "${TC_BUILD_ID}" \
  --zones "us-central1-b,us-west1-b,europe-west2-b" \
  --cockroach "$PWD/bin/cockroach" \
  --workload "$PWD/bin/workload" \
  --artifacts "/artifacts/${artifacts_subdir}" \
  --artifacts-literal="${artifacts}" \
  --parallelism 5 \
  --metamorphic-encryption-probability=0.5 \
  --teamcity || exit_status=$?

if [[ ${exit_status} -eq 10 ]]; then
  # Exit code 10 indicates that some tests failed, but that roachtest
  # as a whole passed. We want to exit zero in this case so that we
  # can let TeamCity report failing tests without also failing the
  # build. That way, build failures can be used to notify about serious
  # problems that prevent tests from being invoked in the first place.
  exit_status=0
fi

# Upload any stats.json files to the cockroach-nightly bucket.
for file in $(find ${artifacts#${PWD}/} -name stats.json); do
    gsutil cp ${file} gs://cockroach-nightly/${file}
done

exit "$exit_status"
