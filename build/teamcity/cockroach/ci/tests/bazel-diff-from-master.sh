#!/usr/bin/env bash

set -xeuo pipefail

impacted_targets_path=$1
workspace_path=$2
starting_hashes_json="/tmp/starting_hashes.json"
final_hashes_json="/tmp/final_hashes.json"

# Generate hashes from current revision.
bazel run --java_runtime_version=remotejdk_11 //:bazel-diff -- generate-hashes -w $workspace_path -b $(which bazel) $final_hashes_json
# Clone to avoid being left in an unexpected branch if we exit unexpectedly.
git clone $(git config --get remote.origin.url) "/tmp/cockroach-previous"
# Generate hashes from master.
bazel run --java_runtime_version=remotejdk_11 //:bazel-diff -- generate-hashes -w "/tmp/cockroach-previous" -b $(which bazel) $starting_hashes_json
rm -rf /tmp/cockroach-previous
# Compare hashes to get affected targets.
bazel run --java_runtime_version=remotejdk_11 //:bazel-diff -- get-impacted-targets -sh $starting_hashes_json -fh $final_hashes_json -o $impacted_targets_path
