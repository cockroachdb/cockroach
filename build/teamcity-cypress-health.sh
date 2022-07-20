#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Build Docker image"
# Buffer noisy output and only print it on failure.
DOCKER_BUILDKIT=1 docker build \
  -f ./pkg/ui/workspaces/e2e-tests/Dockerfile \
  -t cockroachdb/cockroach-ci-ui \
  --progress=plain \
  $PWD &> artifacts/docker-build.log || (cat artifacts/docker-build.log && false)
rm artifacts/docker-build.log
tc_end_block "Build Docker image"

# Expect the timeout to come from the TC environment.
TESTTIMEOUT=${TESTTIMEOUT:-20m}

tc_start_block "Prepare cockroach binary"
# TeamCity doesn't restore permissions for files retrieved from artifact
# dependencies, so ensure the cockroach binary is executable before running it
# in a Docker container.
chmod a+x upstream_artifacts/cockroach
tc_end_block "Prepare cockroach binary"

tc_start_block "Run Cypress health checks"
docker run \
  --rm \
  -v $PWD/upstream_artifacts:/upstream_artifacts \
  -v $PWD/artifacts:/artifacts \
  cockroachdb/cockroach-ci-ui \
  --reporter teamcity \
  --spec 'cypress/e2e/health-check/**'
tc_end_block "Run Cypress health checks"
