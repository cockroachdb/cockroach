#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# You MUST run prepare-summarize-build.sh before running this script.

set -euxo pipefail

if [ ! -f $1 ]
then
    echo 'No file found to upload'
    exit 0
fi

THIS_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

if [ ! -f _bazel/bin/pkg/cmd/bazci/bazel-github-helper/bazel-github-helper_/bazel-github-helper ]
then
    echo 'bazel-github-helper not found'
    exit 1
fi

_bazel/bin/pkg/cmd/bazci/bazel-github-helper/bazel-github-helper_/bazel-github-helper -eventsfile $1 -servername mesolite -cert /home/agent/engflow.crt -key /home/agent/engflow.key -jsonout test-results.json
gcloud storage cp test-results.json gs://engflow-data/$(date +%F)/$(python3 -c "import json, sys; print(json.load(sys.stdin)['invocation_id'])" < test-results.json).json
rm test-results.json
