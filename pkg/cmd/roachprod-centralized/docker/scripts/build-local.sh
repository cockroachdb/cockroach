#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# Script to build roachprod-centralized Docker image locally using Podman
# Supports single-arch and multi-arch builds

# root is the absolute path to the root directory of the repository.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
docker_dir="$(cd "$script_dir/.." &> /dev/null && pwd)"
root="$(cd "$docker_dir/../../../.." &> /dev/null && pwd)"

source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

# Default values
IMAGE="${IMAGE:-us-central1-docker.pkg.dev/cockroach-testeng-infra/roachprod/roachprod-centralized}"
MULTI_ARCH=false
ARCH=""
PUSH=false

# Determine image tag based on git state
SHA=$(git rev-parse --short HEAD)
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    # Working directory has uncommitted changes
    SHA="${SHA}-dirty"
    echo "Warning: Working directory has uncommitted changes"
    echo "Image will be tagged as: ${SHA}"
fi

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --multi-arch)
            MULTI_ARCH=true
            shift
            ;;
        --arch)
            ARCH="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --multi-arch         Build for both amd64 and arm64 (creates manifest)"
            echo "  --arch ARCH          Build for specific architecture (amd64 or arm64)"
            echo "  --push               Push image to registry after build"
            echo "  --help               Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  IMAGE                Image name (default: us-central1-docker.pkg.dev/cockroach-testeng-infra/roachprod/roachprod-centralized)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate arguments
if [[ "$MULTI_ARCH" == "true" && -n "$ARCH" ]]; then
    echo "Error: Cannot specify both --multi-arch and --arch"
    exit 1
fi

if [[ -n "$ARCH" && "$ARCH" != "amd64" && "$ARCH" != "arm64" ]]; then
    echo "Error: --arch must be either amd64 or arm64"
    exit 1
fi

# If no architecture specified and not multi-arch, use host architecture
if [[ "$MULTI_ARCH" == "false" && -z "$ARCH" ]]; then
    HOST_ARCH=$(uname -m)
    if [[ "$HOST_ARCH" == "x86_64" ]]; then
        ARCH="amd64"
    elif [[ "$HOST_ARCH" == "aarch64" || "$HOST_ARCH" == "arm64" ]]; then
        ARCH="arm64"
    else
        echo "Error: Unsupported host architecture: $HOST_ARCH"
        exit 1
    fi
    echo "No architecture specified, using host architecture: $ARCH"
fi

# Build single-arch image
build_single_arch() {
    local arch=$1
    local bazel_config
    local platform

    if [[ "$arch" == "amd64" ]]; then
        bazel_config="crosslinux"
        platform="linux/amd64"
    elif [[ "$arch" == "arm64" ]]; then
        bazel_config="crosslinuxarm"
        platform="linux/arm64"
    fi

    echo "Building ${IMAGE}:${SHA} for ${arch}..."
    echo "Using local source from: $root"

    podman build \
        --format docker \
        --platform "$platform" \
        --tag "${IMAGE}:${SHA}-${arch}" \
        --build-arg BAZEL_IMAGE="$BAZEL_IMAGE" \
        --build-arg BAZEL_CONFIG="$bazel_config" \
        --target final-local \
        --file "$docker_dir/Dockerfile" \
        --ignorefile "$root/.gitignore" \
        "$root"

    echo "Successfully built ${IMAGE}:${SHA}-${arch}"
}

# Main build logic
if [[ "$MULTI_ARCH" == "true" ]]; then
    echo "Building multi-architecture image ${IMAGE}:${SHA}"

    # Clean up any previous manifest
    podman manifest rm "${IMAGE}:${SHA}" 2>/dev/null || true

    # Build for both architectures
    build_single_arch "amd64"
    build_single_arch "arm64"

    # Create manifest
    echo "Creating multi-arch manifest..."
    podman manifest create "${IMAGE}:${SHA}"
    podman manifest add "${IMAGE}:${SHA}" "${IMAGE}:${SHA}-amd64"
    podman manifest add "${IMAGE}:${SHA}" "${IMAGE}:${SHA}-arm64"

    echo "Multi-arch manifest created: ${IMAGE}:${SHA}"

    if [[ "$PUSH" == "true" ]]; then
        echo "Pushing manifest to registry..."
        podman manifest push "${IMAGE}:${SHA}"
        echo "Successfully pushed ${IMAGE}:${SHA}"
    fi
else
    # Single-arch build
    build_single_arch "$ARCH"

    # Tag without arch suffix for convenience
    podman tag "${IMAGE}:${SHA}-${ARCH}" "${IMAGE}:${SHA}"
    echo "Also tagged as ${IMAGE}:${SHA}"

    if [[ "$PUSH" == "true" ]]; then
        echo "Pushing image to registry..."
        podman push "${IMAGE}:${SHA}"
        echo "Successfully pushed ${IMAGE}:${SHA}"
    fi
fi

echo ""
echo "Build complete!"
echo "Image: ${IMAGE}:${SHA}"
if [[ "$PUSH" == "false" ]]; then
    echo ""
    echo "To push the image, run:"
    echo "  podman push ${IMAGE}:${SHA}"
fi
