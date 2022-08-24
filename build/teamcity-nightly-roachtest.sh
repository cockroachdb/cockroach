#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

# Entry point for the nightly roachtests. These are run from CI and require
# appropriate secrets for the ${CLOUD} parameter (along with other things,
# apologies, you're going to have to dig around for them below or even better
# yet, look at the job).

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly $(date)" -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

# Disable global -json flag.
export PATH=$PATH:$(GOFLAGS=; go env GOPATH)/bin

build/builder/mkrelease.sh amd64-linux-gnu build bin/workload bin/roachtest \
  > "${artifacts}/build.txt" 2>&1 || (cat "${artifacts}/build.txt"; false)

# Set up GCE authentication, artifact upload logic, and the PARALLELISM/CPUQUOTA/TESTS env variables.
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --metamorphic-encryption-probability=0.5 \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --build-tag="${BUILD_TAG}" \
  --cockroach="${PWD}/cockroach-linux-2.6.32-gnu-amd64" \
  --artifacts="${artifacts}" \
  --slack-token="${SLACK_TOKEN}" \
  "${TESTS}"
