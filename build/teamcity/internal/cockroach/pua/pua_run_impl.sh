#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

export ROACHPROD_DISABLED_PROVIDERS=aws,azure,ibm
export ROACHPROD_DISABLE_UPDATE_CHECK=true

export ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT=${GOOGLE_SERVICE_ACCOUNT:-teamcity-pua@cockroach-ephemeral.iam.gserviceaccount.com)}
export ROACHPROD_GCE_DEFAULT_PROJECT=${GOOGLE_PROJECT:-cockroach-ephemeral}

export ROACHPROD_DNS=${ROACHPROD_DNS:-roachprod.crdb.io}
export ROACHPROD_GCE_DNS_ZONE=${ROACHPROD_GCE_DNS_ZONE:-roachprod}
export ROACHPROD_GCE_DNS_DOMAIN=${ROACHPROD_GCE_DNS_DOMAIN:-roachprod.crdb.io}
export ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT:-cockroach-ephemeral}

# Check if variable DD_API_KEY is set
if [[ -z "${DD_API_KEY:-}" ]]; then
  echo "error: DD_API_KEY is not set" >&2
  exit 1
fi

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
if [[ "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" ]]; then
  echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > creds.json
  gcloud auth activate-service-account --key-file=creds.json

  # Set GOOGLE_APPLICATION_CREDENTIALS so that gcp go libraries can find it.
  export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/creds.json"
else
  echo 'warning: $GOOGLE_APPLICATION_CREDENTIALS_CONTENT not set' >&2
  exit 1
fi

# build the binaries - roachprod, roachtest and drtprod
build() {
  config="crosslinux"
  # Prepare the bin/ and artifacts/ directories.
  mkdir -p bin artifacts
  chmod o+rwx bin artifacts

  # Array of arguments to be passed to bazel for the component.
  bazel_args=()

  # Array of build artifacts. Each item has format "src:dest"; src is relative to
  # the bazel-bin directory, dst is relative to cwd.
  artifacts=()

  bazel_args+=(//pkg/cmd/roachtest)
  artifacts+=("pkg/cmd/roachtest/roachtest_/roachtest:bin/roachtest")
  artifacts+=("pkg/cmd/roachtest/roachtest_/roachtest:artifacts/roachtest")

  bazel_args+=(//pkg/cmd/roachprod)
  artifacts+=("pkg/cmd/roachprod/roachprod_/roachprod:bin/roachprod")
  artifacts+=("pkg/cmd/roachprod/roachprod_/roachprod:artifacts/roachprod")

  bazel_args+=(//pkg/cmd/drtprod)
  artifacts+=("pkg/cmd/drtprod/drtprod_/drtprod:bin/drtprod")
  artifacts+=("pkg/cmd/drtprod/drtprod_/drtprod:artifacts/drtprod")

  bazel build --config $config -c opt "${bazel_args[@]}"
  BAZEL_BIN=$(bazel info bazel-bin --config $config -c opt)
  for artifact in "${artifacts[@]}"; do
    src=${artifact%%:*}
    dst=${artifact#*:}
    cp "$BAZEL_BIN/$src" "$dst"
    # Make files writable to simplify cleanup and copying (e.g., scp retry).
    chmod a+w "$dst"
  done

  # add bin to path
  export PATH=$PATH:$(pwd)/bin
}

# Run the build function
build

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "teamcity-pua-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

export config=${PUA_CONFIG:-"single_region"}
if [[ "$config" == "single_region" ]]; then
  drtprod execute pkg/cmd/drtprod/configs/drt_pua_9.yaml
else
  # If the config is not single_region, we assume it's multi-region.
  drtprod execute pkg/cmd/drtprod/configs/drt_pua_mr.yaml
fi
