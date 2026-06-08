#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Runs inside the bazel builder container (via run_bazel). Materializes
# the dependency graph with `bazel fetch`, then runs the prebuilt sbom
# tool to write the SBOM, third-party notices, and license-types list
# into /artifacts. Strict validation, Blue Oak non-compliance, and
# copyleft license conflicts each abort the run with a non-zero exit.
#
# The sbom tool itself is built on the host (the builder container has no
# Go toolchain on PATH) and dropped into the artifacts dir, which is
# mounted at /artifacts here.
#
# Required env (set by the wrapper and forwarded into the container):
#   version             release version, e.g. v26.1.0
#   sbom_file           output SBOM filename
#   license_types_file  output license-types filename
#
# BAZEL_STORAGE_ACCESS_TOKEN is also forwarded for `bazel fetch`; the
# script intentionally does NOT use set -x so it never reaches the log.

set -euo pipefail

: "${version:?must be set by the wrapper}"
: "${sbom_file:?must be set by the wrapper}"
: "${license_types_file:?must be set by the wrapper}"

# Materialize every external Go/npm repo on disk so the tool's license
# walker can read each package's metadata. `bazel fetch` downloads and
# extracts only; it runs no build actions.
bazel fetch //pkg/cmd/cockroach

/artifacts/cockroach-sbom \
  --validate \
  --check-blueoak --fail-on-blueoak \
  --check-conflicts \
  --version "${version}" \
  --output        "/artifacts/${sbom_file}" \
  --notices       "/artifacts/THIRD-PARTY-NOTICES.txt" \
  --license-types "/artifacts/${license_types_file}" \
  //pkg/cmd/cockroach
