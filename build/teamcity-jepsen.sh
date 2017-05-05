#!/usr/bin/env bash
set -euxo pipefail
COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
source "${COCKROACH_PATH}/build/jepsen-common.sh"

# This script provisions a Jepsen controller and 5 nodes, and runs tests
# against them.

COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"

$BASH "${COCKROACH_PATH}/build/teamcity-jepsen-prepare.sh"
$BASH "${COCKROACH_PATH}/build/teamcity-jepsen-run.sh"
$BASH "${COCKROACH_PATH}/build/teamcity-jepsen-cleanup.sh"
