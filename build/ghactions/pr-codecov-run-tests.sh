#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

output_json_file="$1"
packages="$2"

if [ -z "${packages}" ]; then
  echo "No packages; skipping"
  touch "${output_json_file}"
  exit 0
fi


# Find the targets. We need to convert from, e.g.
#   pkg/util/log/logpb pkg/util/quotapool
# to
#   //pkg/util/log/logpb:* + //pkg/util/quotapool:*

paths=""
sep=""
for p in ${packages}; do
  # Check if the path is really a package in this tree. We do this by checking
  # for a BUILD.bazel file.
  if [ -f "${p}/BUILD.bazel" ]; then
    paths="${paths}${sep}//${p}:*"
    sep=" + "
  fi
done

targets=""
if [ -n "${paths}" ]; then
  targets=$(bazel query "kind(\".*_test\", ${paths})")
fi

if [[ -z "${targets}" ]]; then
  echo "No test targets found"
  exit 0
fi

echo "Running tests"

# TODO(radu): do we need --strip=never?
bazel coverage \
  --config=crosslinux \
  --@io_bazel_rules_go//go/config:cover_format=lcov --combined_report=lcov \
  --instrumentation_filter="//pkg/..." \
  ${targets}

lcov_file="$(bazel info output_path)/_coverage/_coverage_report.dat"
if [ ! -f "${lcov_file}" ]; then
  echo "Coverage file ${lcov_file} does not exist"
  exit 1
fi

echo "Converting coverage file"
bazel run @go_sdk//:bin/go -- run github.com/cockroachdb/code-cov-utils/lcov2json@v1.0.0 "${lcov_file}" "${output_json_file}"
