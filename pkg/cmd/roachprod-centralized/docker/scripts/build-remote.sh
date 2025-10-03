#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# Script to build roachprod-centralized Docker image remotely using Google Cloud Build
# This builds amd64 images only (Cloud Build doesn't handle arm64 emulation well)

# root is the absolute path to the root directory of the repository.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
docker_dir="$(cd "$script_dir/.." &> /dev/null && pwd)"
root="$(cd "$docker_dir/../../../.." &> /dev/null && pwd)"

source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

# Default values
OWNER="${OWNER:-cockroachdb}"
REPO="${REPO:-cockroach}"
SHA=$(git rev-parse --short HEAD)
IMAGE=us-central1-docker.pkg.dev/cockroach-testeng-infra/roachprod/roachprod-centralized


# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --owner)
            OWNER="$2"
            shift 2
            ;;
        --repo)
            REPO="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --owner            GitHub owner (default: cockroachdb)"
            echo "  --repo             GitHub repo (default: cockroach)"
            echo "  --help             Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  OWNER              GitHub owner (default: cockroachdb)"
            echo "  REPO               GitHub repo (default: cockroach)"
            echo "  IMAGE              Image name (default: us-central1-docker.pkg.dev/cockroach-testeng-infra/roachprod/roachprod-centralized)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "Building image ${IMAGE}:${SHA} using Google Cloud Build (amd64 only)..."

gcloud --project cockroach-testeng-infra builds submit \
  --config "$docker_dir/cloudbuild.yaml" \
  --substitutions=_BAZEL_IMAGE="$BAZEL_IMAGE",_SHA="$SHA",_OWNER="$OWNER",_REPO="$REPO" \
  --timeout=30m

echo ""
echo "Remote build complete!"
echo "Image: ${IMAGE}:${SHA}"