#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Root directory to start searching
ROOT_DIR=${1:-.}  # defaults to current directory if not specified

# Function to extract and format benchmark data
extract_benchmark_data() {
    local file_path="$1"
    local bench_prefix

    # Extract the benchmark path, trimming to tpcc-nowait/.../cpu=16 (any strategy in place of "literal"/"optimized")
    bench_prefix=$(echo "$file_path" | sed -E 's|.*/(tpcc-nowait/[^/]+/w=[^/]+/nodes=[^/]+/cpu=[^/]+)/run_[^/]+/.*|\1|')

    # Process only after first "total" header and stop if "tpmC" is seen
    awk -v prefix="$bench_prefix" '
    /tpmC/ { exit }                      # stop processing after tpmC
    /total/ { found_total = 1 }
    !found_total { next }

    /^_elapsed___errors/ {
        header = 1
        next
    }
    header == 1 && /^[[:space:]]*[0-9]+\.[0-9]+s/ {
        # Capture values and label (last column)
        elapsed = $1
        ops_total = $3
        ops_sec = $4
        avg_ms = $5
        p50 = $6
        p95 = $7
        p99 = $8
        pmax = $9
        label = $10

        # Remove units and convert label to lowercase
        gsub(/s$/, "", elapsed)
        label_lc = tolower(label)
        if (label_lc == "") label_lc = "total"

        printf "BenchmarkTPCC/%s/%-12s %10s %8s ops/sec %8s ms/avg %8s ms/p50 %8s ms/p95 %8s ms/p99 %8s ms/max\n", \
            prefix, label_lc, ops_total, ops_sec, avg_ms, p50, p95, p99, pmax
    }
    ' "$file_path"
}

export -f extract_benchmark_data

# Find all matching files and process them
find "$ROOT_DIR" -type f -path "*/tpcc-nowait/*/w=*/nodes=*/cpu=*/run_*/"*"_cockroach-workload-r.log" \
    -exec bash -c 'extract_benchmark_data "$0"' {} \;
