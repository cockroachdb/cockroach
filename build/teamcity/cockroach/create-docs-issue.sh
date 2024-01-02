#!/usr/bin/env bash

# This script is called by the build configuration
# "Cockroach > Create Docs Issue" in TeamCity.

set -euxo pipefail

dir="$(dirname $(dirname $(dirname "${0}")))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GITHUB_API_TOKEN -e JIRA_API_TOKEN -e DOCS_ISSUE_GEN_END_TIME -e DOCS_ISSUE_GEN_START_TIME -e DRY_RUN_DOCS_ISSUE_GEN" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/docs-issue-generation
BAZEL_BIN=$(bazel info bazel-bin --config ci)
$BAZEL_BIN/pkg/cmd/docs-issue-generation/docs-issue-generation_/docs-issue-generation
EOF
