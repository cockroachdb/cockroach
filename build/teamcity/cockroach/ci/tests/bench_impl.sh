#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity/util.sh"

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

# Enumerate test targets that have benchmarks.
all_tests=$(bazel query 'kind(go_test, //pkg/...)' --output=label)
pkgs=$(git grep -l '^func Benchmark' -- 'pkg/*_test.go' | rev | cut -d/ -f2- | rev | sort | uniq)
targets=$(for pkg in $pkgs
          do
              pkg=$(echo $pkg | sed 's|^|\^//|' | sed 's|$|:|')
              grep $pkg <<< $all_tests || true
          done | sort | uniq)

set -x
# Run all tests serially.
for target in $targets
do
    tc_start_block "Bench $target"
    # We need the `test_sharding_strategy` flag or else the benchmarks will
    # fail to run sharded tests like //pkg/sql/importer:importer_test.
    bazel run --config=test --config=$CROSSLINUX_CONFIG --config=ci --test_sharding_strategy=disabled $target -- \
          -test.bench=. -test.benchtime=1x -test.short -test.run=-
    tc_end_block "Bench $target"
done
