#!/usr/bin/env bash
function build_docker_image() {
  # Buffer noisy output and only print it on failure.
  DOCKER_BUILDKIT=1 run docker build \
    -f ./pkg/ui/workspaces/e2e-tests/Dockerfile \
    -t cockroachdb/cockroach-cypress \
    --progress=plain \
    $PWD &> artifacts/docker-build.log || (cat artifacts/docker-build.log && false)
  rm artifacts/docker-build.log
}

function run_tests() {
  SPEC_ARG=""
  if [ "health" = "${1:-'EMPTY'}" ]; then
    SPEC_ARG="--spec 'cypress/e2e/health-check/**'"
  fi

  run docker run \
    --rm \
    -v $PWD/upstream_artifacts:/upstream_artifacts \
    -v $PWD/artifacts:/artifacts \
    cockroachdb/cockroach-cypress \
    --reporter teamcity \
    $SPEC_ARG
}
