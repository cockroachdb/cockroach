#!/usr/bin/env bash
function load_cockroach_docker_image() {
  set -x
  which docker && docker --version
  which docker-compose && docker-compose --version
  set +x
  docker load --input upstream_artifacts/cockroach-docker-image.tar.gz &> artifacts/docker-load.log || (cat artifacts/docker-load.log && false)
  rm artifacts/docker-load.log
}

function run_tests() {
  SPEC_ARG=""
  if [ "health" = "${1:-'EMPTY'}" ]; then
    SPEC_ARG="--spec 'cypress/e2e/health-check/**'"
  fi

  run docker compose run cypress -- $SPEC_ARG
}
