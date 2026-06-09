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
# Everything runs on the host. The workflow installs Go and Bazel
# (bazelisk) and builds the sbom tool into the artifacts dir. The tool
# needs both bazel (to fetch and query the dependency graph) and go (for
# `go mod graph`) on PATH, so it runs here rather than inside the bazel
# builder container, which has no Go toolchain. The host is already
# authenticated to gcloud by the workflow's auth step.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$dir/build/teamcity-support.sh"        # for $root, tc_start_block

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
# Materialize every external Go/npm repo on disk so the tool's license
# walker can read each package's metadata, then run the tool. `bazel
# fetch` downloads and extracts only; it runs no build actions. bazel and
# the tool run from the repo root so the //pkg/cmd/cockroach target
# resolves; the Blue Oak rating check is intentionally not run
# (--validate and --check-conflicts still gate the release).
(
  cd "$root"
  bazel fetch //pkg/cmd/cockroach
  "$root/artifacts/cockroach-sbom" \
    --validate \
    --check-conflicts \
    --version       "${version}" \
    --output        "$root/artifacts/${sbom_file}" \
    --notices       "$root/artifacts/THIRD-PARTY-NOTICES.txt" \
    --license-types "$root/artifacts/${license_types_file}" \
    //pkg/cmd/cockroach
)
tc_end_block "Generate SBOM, notices, and license-types"

tc_start_block "Compare third-party notices"
# Compare the generated notices against the checked-in copy. Drift is
# surfaced as a warning rather than a hard failure: regenerating
# licenses/THIRD-PARTY-NOTICES.txt should be a deliberate, reviewed
# change, not something that blocks a release build.
if ! diff -u "$root/licenses/THIRD-PARTY-NOTICES.txt" "$root/artifacts/THIRD-PARTY-NOTICES.txt"; then
  echo "::warning::generated THIRD-PARTY-NOTICES.txt differs from licenses/THIRD-PARTY-NOTICES.txt; consider regenerating and updating it."
fi
# Keep the generated notices in the artifacts dir; the workflow publishes
# it (alongside the SBOM and license-types list) as a build artifact.
tc_end_block "Compare third-party notices"

tc_start_block "Upload SBOM and license-types"
# The host is already authenticated to gcloud by the workflow's
# google-github-actions/auth + setup-gcloud steps.
gcloud storage cp \
  "$root/artifacts/${sbom_file}" \
  "$root/artifacts/${license_types_file}" \
  "gs://${gcs_bucket}/"
tc_end_block "Upload SBOM and license-types"
