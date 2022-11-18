#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run unit tests"
if [[ _tc_build_branch =~ ^[0-9]+$ ]] || [[ tc_bors_branch ]];
then
    affected_targets_tmp_dir=$(mktemp -d)
    impacted_targets_path="$affected_targets_tmp_dir/impacted_targets.txt"
    impacted_targets_docker_path="/tmp/impacted_targets.txt"
    build/teamcity/cockroach/ci/tests/bazel-diff-from-master.sh $impacted_targets_path $root
    grep -v -E '^//pkg:small_tests$|^//pkg:medium_tests$|^//pkg:large_tests$|^//pkg:enormous_tests$|^//pkg:all_tests$' $impacted_targets_path
    comm -12 <(sort build/teamcity/cockroach/ci/ALL_TESTS) <(sort $impacted_targets_path) > $impacted_targets_path
    targets_to_test="--target_pattern_file=$impacted_targets_docker_path"
else
    targets_to_test="//pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests"
fi
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e targets_to_test=$targets_to_test -v $impacted_targets_path:$impacted_targets_docker_path" run_bazel build/teamcity/cockroach/ci/tests/unit_tests_impl.sh
tc_end_block "Run unit tests"
