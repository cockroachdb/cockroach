#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

google_credentials="$GOOGLE_CREDENTIALS"
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "$dir/../../../teamcity-support.sh"
log_into_gcloud

mkdir -p artifacts

# NB: The certs are set up and cleaned up in unconditional build steps before
# and after this build step.
ENGFLOW_FLAGS="--config engflow --config crosslinux \
--jobs 400 --tls_client_certificate=/home/agent/engflow/engflow.crt \
--tls_client_key=/home/agent/engflow/engflow.key"

BES_KEYWORDS_ARGS=
if [ ! -z "${EXTRA_ISSUE_PARAMS:+$EXTRA_ISSUE_PARAMS}" ]
then
    BES_KEYWORDS_ARGS=$(echo "$EXTRA_ISSUE_PARAMS" | sed 's/,/\n/g' | sed 's/^/--bes_keywords /g' | tr '\n' ' ')
fi

status=0
bazel test //pkg:all_tests $ENGFLOW_FLAGS --remote_download_minimal \
      --runs_per_test ${RUNS_PER_TEST=10} --verbose_failures --build_event_binary_file=artifacts/eventstream \
      --profile=artifacts/profile.json.gz \
      ${EXTRA_TEST_ARGS:+$EXTRA_TEST_ARGS} \
      $BES_KEYWORDS_ARGS \
      --bes_keywords "branch=${TC_BUILD_BRANCH#refs/heads/}" \
      --bes_keywords nightly_stress \
    || status=$?

# Upload results to GitHub.
bazel build //pkg/cmd/bazci/process-bep-file $ENGFLOW_FLAGS --bes_keywords helper-binary
_bazel/bin/pkg/cmd/bazci/process-bep-file/process-bep-file_/process-bep-file \
    -eventsfile artifacts/eventstream \
    -cert /home/agent/engflow/engflow.crt -key /home/agent/engflow/engflow.key \
    -extra "${EXTRA_ISSUE_PARAMS:+$EXTRA_ISSUE_PARAMS}" \
    -jsonoutfile test-results.json

gcloud storage cp test-results.json gs://engflow-data/$(date +%F)/$(python3 -c "import json, sys; print(json.load(sys.stdin)['invocation_id'])" < test-results.json).json

rm test-results.json

exit $status
