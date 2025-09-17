#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -exuo pipefail

export ROACHPROD_DISABLED_PROVIDERS=aws,azure,ibm
export ROACHPROD_DISABLE_UPDATE_CHECK=true

export ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT=${GOOGLE_SERVICE_ACCOUNT:-teamcity-pua@cockroach-ephemeral.iam.gserviceaccount.com}
export ROACHPROD_GCE_DEFAULT_PROJECT=${GOOGLE_PROJECT:-cockroach-ephemeral}

export ROACHPROD_DNS=${ROACHPROD_DNS:-roachprod.crdb.io}
export ROACHPROD_GCE_DNS_ZONE=${ROACHPROD_GCE_DNS_ZONE:-roachprod}
export ROACHPROD_GCE_DNS_DOMAIN=${ROACHPROD_GCE_DNS_DOMAIN:-roachprod.crdb.io}

# generate the ssh key if it doesn't exist.
if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "teamcity-pua-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

# set up google credentials.
if [[ "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" ]]; then
  echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > creds.json
  gcloud auth activate-service-account --key-file=creds.json

  # Set GOOGLE_APPLICATION_CREDENTIALS so that gcp go libraries can find it.
  export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/creds.json"
else
  echo 'warning: $GOOGLE_APPLICATION_CREDENTIALS_CONTENT not set' >&2
  exit 1
fi

# build the binaries: roachprod, roachtest, and drtprod.
build() {
  config="crosslinux"
  # prepare the bin/ and artifacts/ directories.
  mkdir -p bin artifacts
  chmod o+rwx bin artifacts

  # array of arguments to be passed to bazel for the component.
  bazel_args=()

  # array of build artifacts. each item has format "src:dest"; src is relative to
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

  # add bin to path.
  export PATH=$PATH:$(pwd)/bin
}

# run the build function.
build

log_file="artifacts/pua.log"
export config=${PUA_CONFIG:-"single_region"}
if [[ "$config" == "single_region" ]]; then
  CLUSTER=drt-pua-9
  WORKLOAD=workload-pua-9
  ZONE_NODE=7-9
  config_file="pkg/cmd/drtprod/configs/drt_pua_9.yaml"
elif [[ "$config" == "multi_region" ]]; then
  CLUSTER=drt-pua-15
  WORKLOAD=workload-pua-15
  ZONE_NODE=3-4
  config_file="pkg/cmd/drtprod/configs/drt_pua_mr.yaml"
else
  echo "Error: Invalid PUA_CONFIG value: '$config'. Must be 'single_region' or 'multi_region'." >&2
  exit 1
fi

# execute the pua benchmark test.
drtprod execute ${config_file} | tee -a "${log_file}"

# the pua dashboard uses a json file to show the benchmark results.
# we will generate the json file from the datadog metrics.
# download metric converter from gcs bucket pua-backup-us-east1.
mkdir -p datadog-metric-converter
gsutil -m cp -r gs://pua-backup-us-east1/datadog-metric-converter/** datadog-metric-converter/

# install pip for python3.8.
curl -sS https://bootstrap.pypa.io/pip/3.8/get-pip.py -o get-pip.py
python3 get-pip.py

# install the requirements for the metric converter.
python3 -m pip install -r datadog-metric-converter/requirements.txt

# get the start and end time of the benchmark.
epoch_start_time=$(grep "\[Phase-1: Baseline Performance\]" ${log_file} | grep "Starting" | awk -F'[][]' '{print $4}')
epoch_start_time=$((epoch_start_time - 240))
epoch_end_time=$(( $(date +%s) - 120 ))
host=$(hostname)

# generate the benchmark.json file
python3 datadog-metric-converter/convert-datadog-metric.py --start-time=${epoch_start_time} --end-time=${epoch_end_time} \
        --cluster-name ${CLUSTER} --workload-name ${WORKLOAD} \
        --monitor-host ${host} --zone-node ${ZONE_NODE}


# delete the binaries - roachprod, roachtest and drtprod,
# as we don't need them to be uploaded to TeamCity artifacts
rm -f artifacts/roachprod artifacts/roachtest artifacts/drtprod
cp benchmark.json "artifacts/benchmark.json"

rm -rf datadog-metric-converter

# destroy the clusters.
drtprod destroy ${CLUSTER}
drtprod destroy ${WORKLOAD}
