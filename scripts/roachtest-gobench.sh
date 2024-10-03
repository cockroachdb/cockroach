#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# Converts roachtest benchmark results (for kv or ycsb workloads) into Go
# benchmark format, suitable for use with e.g. benchstat.

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <artifacts-dir>"
    exit 1
fi

for file in $(find $1 -name test.log -o -name 'run_*workload-run-*.log'); do
    name=$(dirname $(dirname $(realpath --relative-to=$1 $file)))
    grep -h -A1 __result $file \
        | grep -v '^--$' | grep -v __result | \
        awk "{printf \"Benchmark$name  1  %s ops/sec  %s p50  %s p95  %s p99\n\", \$4, \$6, \$7, \$8}"
done
