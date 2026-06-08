#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper that generates the release SBOM, third-party
# notices, and unique-license-types list via the cockroachlabs/release-eng
# sbom tool, then uploads the SBOM and license-types list to the staged
# release bucket (the same bucket as the tarballs).
#
# The sbom tool is built on the host (see the workflow) and lives in the
# artifacts dir. The heavy work (bazel fetch + running the tool) runs
# inside the bazel builder container via run_bazel, which mounts the
# host's artifacts dir at /artifacts and the GCP credential file. The
# notices comparison and GCS upload run on the host, which is already
# authenticated to gcloud by the workflow's auth step.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$dir/build/teamcity-support.sh"        # for $root, tc_start_block
source "$dir/build/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$root/pkg/build/version.txt" | head -n1)
if ! echo "${version}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$'; then
  echo "Invalid version \"${version}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
  exit 1
fi

if [[ -z "${DRY_RUN:-}" ]]; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
fi

sbom_file="cockroach@${version}.sbom.cyclonedx.json"
license_types_file="cockroach@${version}.list-of-license-types.txt"
tc_end_block "Variable Setup"

tc_start_block "Generate SBOM, notices, and license-types"
# Forward the computed filenames and BAZEL_STORAGE_ACCESS_TOKEN (needed
# by `bazel fetch` to pull private godeps) into the container. Export the
# vars and use value-less `-e` flags so the values pass through the
# environment rather than the docker-args string, which run_bazel
# word-splits (a value with spaces would otherwise break).
export version sbom_file license_types_file
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e version -e sbom_file -e license_types_file -e BAZEL_STORAGE_ACCESS_TOKEN" \
  run_bazel build/github/release-generate-sbom-impl.sh
tc_end_block "Generate SBOM, notices, and license-types"

tc_start_block "Compare third-party notices"
# Compare the generated notices against the checked-in copy. Drift is
# surfaced as a warning rather than a hard failure: regenerating
# licenses/THIRD-PARTY-NOTICES.txt should be a deliberate, reviewed
# change, not something that blocks a release build.
if ! diff -u "$root/licenses/THIRD-PARTY-NOTICES.txt" "$root/artifacts/THIRD-PARTY-NOTICES.txt"; then
  echo "::warning::generated THIRD-PARTY-NOTICES.txt differs from licenses/THIRD-PARTY-NOTICES.txt; consider regenerating and updating it."
fi
# The generated notices are only used for the comparison above; discard.
rm -f "$root/artifacts/THIRD-PARTY-NOTICES.txt"
tc_end_block "Compare third-party notices"

tc_start_block "Upload SBOM and license-types"
# The host is already authenticated to gcloud by the workflow's
# google-github-actions/auth + setup-gcloud steps.
gcloud storage cp \
  "$root/artifacts/${sbom_file}" \
  "$root/artifacts/${license_types_file}" \
  "gs://${gcs_bucket}/"
tc_end_block "Upload SBOM and license-types"
