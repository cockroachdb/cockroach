#!/usr/bin/env bash
set -euxo pipefail
COCKROACH_PATH="${GOPATH}/src/github.com/cockroachdb/cockroach"
source "${COCKROACH_PATH}/build/jepsen-common.sh"

progress Destroying cluster...
terraform destroy --var=key_name="${KEY_NAME}" --force || true
