#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For log_into_gcloud

outputfile="$1"
packages="$2"

# Find the targets. We need to convert from, e.g.
#   ./pkg/util/log/logpb ./pkg/util/quotapool
# to
#   //pkg/util/log/logpb:* + //pkg/util/quotapool:*

paths=""
sep=""

for p in ${packages}; do
  if [[ $p == ./pkg/* ]]; then
    path="//pkg/${p#./pkg/}"
    paths="${paths}${sep}${path}:*"
    sep=" + "
  else
    echo "ignoring $p"
  fi
done

targets=$(bazel query "kind(\".*_test\", ${paths})")

if [[ -z "${targets}" ]]; then
  echo "No test targets found"
  exit 0
fi

echo "Running tests"

bazel coverage --compilation_mode=fastbuild \
  --@io_bazel_rules_go//go/config:cover_format=lcov --combined_report=lcov \
  --instrumentation_filter="//pkg/..." --test_verbose_timeout_warnings \
  ${targets}

lcovfile="$(bazel info output_path)/_coverage/_coverage_report.dat"
if [ ! -f "${lcovfile}" ]; then
  echo "Coverage file ${lcovfile} does not exist"
  exit 1
fi

echo "Converting coverage file"

jsonfile=$(mktemp --suffix=.json)
trap 'rm -f "${jsonfile}"' EXIT

go run github.com/cockroachdb/code-cov-utils/lcov2json@master "${lcovfile}" "${jsonfile}"

log_into_gcloud
gsutil cp "${jsonfile}" "${outputfile}"
