#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_progress_block "Build Docker image"
# Buffer noisy output and only print it on failure.
run DOCKER_BUILDKIT=1 docker build \
  -f ./pkg/ui/workspaces/e2e-tests/Dockerfile \
  -t cockroachdb/cockroach-ci-ui \
  --progress=plain \
  $PWD &> artifacts/docker-build.log || (cat artifacts/docker-build.log && false)
rm artifacts/docker-build.log
tc_end_progress_block "Build Docker image"

# Expect the timeout to come from the TC environment.
TESTTIMEOUT=${TESTTIMEOUT:-20m}

# TeamCity doesn't restore permissions for files retrieved from artifact
# dependencies, so ensure the cockroach binary is executable before running it
# in a Docker container.
chmod a+x upstream_artifacts/cockroach

tc_start_progress_block "Run Cypress health checks"
run docker run \
  --rm \
  -v $PWD/upstream_artifacts:/upstream_artifacts \
  -v $PWD/artifacts:/artifacts \
  cockroachdb/cockroach-ci-ui \
  --reporter teamcity \
  --spec 'cypress/e2e/health-check/**'
tc_end_progress_block "Run Cypress health checks"
