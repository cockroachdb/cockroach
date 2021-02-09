#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/../shlib.sh"

tc_start_block "Variable Setup"
source "$(dirname "${0}")/release-support.sh"
tc_end_block "Variable Setup"

tc_start_block "Configure docker"
configure_docker_creds
docker_login_with_google
docker_login
tc_end_block "Configure docker"

tc_start_block "Pull, tag and push GCR docker image"
retry docker pull "${dockerhub_repository}:${build_name}"
docker tag "${dockerhub_repository}:${build_name}" "${gcr_repository}:${build_name}"
retry docker push "${gcr_repository}:${build_name}"
tc_end_block "Pull, tag and push GCR docker image"
