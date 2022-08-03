#!/usr/bin/env bash
function load_cockroach_docker_image() {
  docker load /upstream_artifacts/cockroach-docker-image.tar.gz &> artifacts/docker-load.log || (cat artifacts/docker-load.log && false)
  rm artifacts/docker-load.log
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
