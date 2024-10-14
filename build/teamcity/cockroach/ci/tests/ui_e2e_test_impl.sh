#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

function load_cockroach_docker_image() {
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
