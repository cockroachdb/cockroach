#!/usr/bin/env bash

set -exuo pipefail

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source "$dir/../../../../teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-stress $(date)" -N "" -f ~/.ssh/id_rsa
fi

source "$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh"

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
  --cockroach="${PWD}/bin/cockroach" \
  --artifacts="${PWD}/artifacts" \
  --disable-issue \
  "${TESTS}"
