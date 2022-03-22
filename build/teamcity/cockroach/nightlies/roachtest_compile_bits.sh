#!/usr/bin/env bash

# Builds all bits needed for roachtests, stages them in bin/ and lib.docker_amd64/.

bazel build --config crosslinux --config ci --config with_ui -c opt \
      //pkg/cmd/cockroach //pkg/cmd/workload //pkg/cmd/roachtest \
      //c-deps:libgeos
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux --config ci --config with_ui -c opt)
# Move this stuff to bin for simplicity.
mkdir -p bin
chmod o+rwx bin
cp $BAZEL_BIN/pkg/cmd/cockroach/cockroach_/cockroach bin
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
cp $BAZEL_BIN/pkg/cmd/workload/workload_/workload    bin
chmod a+w bin/cockroach bin/roachtest bin/workload
# Stage the geos libs in the appropriate spot.
mkdir -p lib.docker_amd64
chmod o+rwx lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos.so   lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos_c.so lib.docker_amd64
chmod a+w lib.docker_amd64/libgeos.so lib.docker_amd64/libgeos_c.so
ln -s lib.docker_amd64 lib
