#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

build="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
# for tc_prepare, tc_start_block, and friends
source "$build/teamcity-support.sh"
# for build_docker_image and run_tests
source "$build/teamcity/cockroach/ci/tests/ui_e2e_test_impl.sh"

tc_prepare

tc_start_block "Load cockroachdb/cockroach-ci image"
load_cockroach_docker_image
tc_end_block "Load cockroachdb/cockroach-ci image"

tc_start_block "Run Cypress health checks"
cd $root/pkg/ui/workspaces/e2e-tests
run_tests health
tc_end_block "Run Cypress health checks"
