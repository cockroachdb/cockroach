#!/usr/bin/env bash

set -xeuo pipefail

# Usage: testrace_impl.sh PKG1 [PKG2 PKG3 PKG4...]
# packages are expected to be formatted as go-style, e.g. ./pkg/cmd/bazci.

bazel build //pkg/cmd/bazci --config=ci
for pkg in "$@"
do
    # Query to list all affected tests.
    pkg=${pkg#"./"}
    if [[ $(basename $pkg) != ... ]]
    then
        pkg="$pkg:all"
    fi
    tests=$(bazel query "kind(go_test, $pkg)" --output=label)

    # Run affected tests.
    for test in $tests
    do
        if [[ ! -z $(bazel query "attr(tags, \"broken_in_bazel\", $test)") ]]
        then
            echo "Skipping test $test as it is broken in bazel"
            continue
        fi
        $(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci --config=race "$test" \
                               --test_env=COCKROACH_LOGIC_TESTS_SKIP=true \
                               --test_env=GOMAXPROCS=8
    done
done

