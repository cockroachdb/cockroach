#!/usr/bin/env bash

set -xeuo pipefail

# Enumerate test targets that have benchmarks.
all_tests=$(bazel query 'kind(go_test, //pkg/...)' --output=label)
pkgs=$(git grep -l '^func Benchmark' -- 'pkg/*_test.go' | rev | cut -d/ -f2- | rev | sort | uniq)
targets=$(for pkg in $pkgs
          do
              pkg=$(echo $pkg | sed 's|^|\^//|' | sed 's|$|:|')
              grep $pkg <<< $all_tests || true
          done | sort | uniq)

# Build all tests, then run them serially.
bazel build --config=test --config=crosslinux $targets
for target in $targets
do
    bazel run --config=test --config=crosslinux $target -- -test.bench=. -test.benchtime=1ns -test.short
done
