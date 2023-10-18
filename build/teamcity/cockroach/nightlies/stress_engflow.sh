#!/usr/bin/env bash

set -euo pipefail

mkdir -p artifacts

# NB: The certs are set up and cleaned up in unconditional build steps before
# and after this build step.
ENGFLOW_FLAGS="--config engflow --config cibase --config crosslinux \
--jobs 400 --tls_client_certificate=/home/agent/engflow/engflow.crt \
--tls_client_key=/home/agent/engflow/engflow.key \
--remote_upload_local_results=false"
INVOCATION_ID=$(uuidgen)

status=0
bazel test //pkg:all_tests $ENGFLOW_FLAGS --runs_per_test 30 \
      --verbose_failures --build_event_binary_file=artifacts/eventstream \
      --profile=artifacts/profile.json.gz --invocation_id=$INVOCATION_ID \
    || status=$?

# Upload results to GitHub.
bazel build //pkg/cmd/bazci/process-bep-file $ENGFLOW_FLAGS
_bazel/bin/pkg/cmd/bazci/process-bep-file/process-bep-file_/process-bep-file \
    -branch $TC_BUILD_BRANCH -eventsfile artifacts/eventstream \
    -invocation $INVOCATION_ID -cert /home/agent/engflow/engflow.crt \
    -key /home/agent/engflow/engflow.key

exit $status
