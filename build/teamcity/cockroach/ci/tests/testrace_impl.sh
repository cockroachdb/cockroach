#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

# Usage: testrace_impl.sh PKG1 [PKG2 PKG3 PKG4...]
# packages are expected to be formatted as go-style, e.g. ./pkg/cmd/bazci.

# Race runs are what report data races; the non-race runs report everything
# else. Teams that set filter_race_runs_to_race_issues in TEAMS.yaml get
# issues from this run only for the former.
export COCKROACH_FILTER_RACE_RUNS_TO_RACE_ISSUES=1

bazel build //pkg/cmd/bazci
size_to_timeout=("small:1200" "medium:6000" "large:18000" "enormous:72000")
for pkg in "$@"
do
    # Query to list all affected tests.
    pkg=${pkg#"./"}
    if [[ $(basename $pkg) != ... ]]
    then
        pkg="$pkg:all"
    fi

    for kv in "${size_to_timeout[@]}";
    do
        size="${kv%%:*}"
        timeout="${kv#*:}"
        tests=$(bazel query "attr(size, $size, kind("go_test", tests($pkg)))" --output=label)
        # Run affected tests.
        for test in $tests
        do
            if [[ ! -z $(bazel query "attr(tags, \"integration\", $test)") ]]
            then
                echo "Skipping test $test"
                continue
            fi
            $(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci --config=race "$test" \
                                --test_env=COCKROACH_LOGIC_TESTS_SKIP=true \
                                --test_timeout $timeout \
                                --test_sharding_strategy=disabled \
                                --test_env=GOMAXPROCS=8
        done
    done
done

