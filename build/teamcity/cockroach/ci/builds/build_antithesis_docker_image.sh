#!/usr/bin/env bash
set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"  # For $root

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

export TMPDIR=$PWD/artifacts/instrument
mkdir -p "$TMPDIR"

tc_start_block "Compile instrumented crdb (short) binary"

run build/builder.sh mkrelease amd64-linux-gnu instrumentshort INSTRUMENTATION_TMP=$TMPDIR
tc_end_block "Compile instrumented crdb (short) binary"

tc_start_block "Copy instrumented binary and dependency files to build/deploy"
# copy instrumented binary, symbol map and uninstrumented binary (used for workload generation)
cp $TMPDIR/go-*.sym.tsv build/deploy/
cp $TMPDIR/cockroach-instrumented build/deploy/instrument
cp $TMPDIR/libvoidstar.so build/deploy/
cp cockroachshort-linux-2.6.32-gnu-amd64 build/deploy/workload
cp -r licenses build/deploy/

chmod 755 build/deploy/instrument
chmod 755 build/deploy/workload
tc_end_block "Copy instrumented binary and dependency files to build/deploy"

tc_start_block "Build cockroach-instrumented docker image"

docker build \
  --no-cache \
  --tag="cockroachdb/cockroach-instrumented:latest" \
  --memory 30g \
  --memory-swap -1 \
  -f build/deploy/Dockerfile_antithesis \
  build/deploy
tc_end_block "Build cockroach-instrumented docker image"

tc_start_block "Build workload docker image"

docker build \
  --no-cache \
  --tag="cockroachdb/workload:latest" \
  --memory 30g \
  --memory-swap -1 \
  -f build/deploy/Dockerfile \
  build/deploy
tc_end_block "Build workload docker image"

tc_start_block "Build docker-compose config. image"
# TODO
tc_end_block "Build docker-compose config. image"

tc_start_block "Pushing docker images to GCR"

docker tag cockroachdb/cockroach-instrumented:latest gcr.io/cockroach-testeng-infra/cockroachdb/cockroach-instrumented:latest
docker tag cockroachdb/workload:latest  gcr.io/cockroach-testeng-infra/cockroachdb/workload:latest
docker push gcr.io/cockroach-testeng-infra/cockroachdb/cockroach-instrumented:latest
docker push gcr.io/cockroach-testeng-infra/cockroachdb/workload:latest
tc_end_block "Pushing docker images to GCR"
