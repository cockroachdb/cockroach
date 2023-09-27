#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud

CORPUS_DIR=/artifacts/logictest-stmts-corpus-dir # dir to store all collected corpus file(s)
exit_status=0

# Collect sql logic tests statements corpus.
bazel run -- //pkg/cmd/generate-logictest-corpus:generate-logictest-corpus \
-out-dir=$CORPUS_DIR  \
|| exit_status=$?

# Persist the validated corpus to GCS.
if [ $exit_status = 0 ]; then
  gsutil cp  $CORPUS_DIR/* gs://cockroach-corpus/corpus-$TC_BUILD_BRANCH/
fi
