#!/usr/bin/env bash

set -euxo pipefail

if [ ! -f $1 ]
then
    echo 'No file found to upload'
    exit 0
fi

THIS_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

bazel build //pkg/cmd/bazci/extract-test-results --config crosslinux --jobs 300 $($THIS_DIR/engflow-args.sh) --bes_keywords helper-binary
_bazel/bin/pkg/cmd/bazci/extract-test-results/extract-test-results_/extract-test-results -eventsfile $1 -servername mesolite -cert /home/agent/engflow.crt -key /home/agent/engflow.key -jsonout test-results.json
gcloud storage cp test-results.json gs://engflow-data/$(date +%F)/$(python3 -c "import json, sys; print(json.load(sys.stdin)['invocation_id'])" < test-results.json).json
rm test-results.json
