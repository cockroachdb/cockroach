#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# N.B. export variables like `root` s.t. they can be used by scripts called below.
set -a
source "$dir/../../../teamcity-support.sh"
set +a

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-stress $(date)" -N "" -f ~/.ssh/id_rsa
fi

os=linux
arch=amd64
if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
  arch=amd64-fips
fi
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh $arch

# NOTE: we use the GOOGLE_CREDENTIALS environment here, rather than the
# "ephemeral" variant. The former is specific to the roachtest stress job,
# whereas the latter (which is an inherited parameter) does not have the
# requisite permissions to create instance in the cockroach-roachstress GCP
# project.
google_credentials="$GOOGLE_CREDENTIALS"
generate_ssh_key
log_into_gcloud

export GCE_PROJECT=${GCE_PROJECT-cockroach-roachstress}

build/teamcity-roachtest-invoke.sh \
  --cloud="${CLOUD-gce}" \
  --zones="${GCE_ZONES-us-central1-b,us-west4-a,europe-west4-c}" \
  --debug="${DEBUG-false}" \
  --count="${COUNT-16}" \
  --parallelism="${PARALLELISM-16}" \
  --cpu-quota="${CPUQUOTA-1024}" \
  --cluster-id="${TC_BUILD_ID}" \
  --lifetime="36h" \
  --cockroach="${PWD}/bin/cockroach.$os-$arch" \
  --artifacts="${PWD}/artifacts" \
  --disable-issue \
  "${TESTS}"
