#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "Builds all bits needed for roachtests and stages them in bin/ and lib/."
  echo ""
  echo "Usage: $0 arch [arch...]"
  echo "  where arch is one of: amd64, arm64, amd64-fips"
  exit 1
fi

os=linux
function arch_to_config() {
  case "$1" in
    amd64)
      echo "crosslinux"
      ;;
    arm64)
      echo "crosslinuxarm"
      ;;
    amd64-fips)
      echo "crosslinuxfips"
      ;;
    *)
      echo "Error: invalid arch '$1'" >&2
      exit 1
      ;;
  esac
}

# Determine host cpu architecture, which we'll need for libgeos, below.
if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  host_arch=arm64
else
  host_arch=amd64
fi
echo "Host architecture: $host_arch"

# Prepare the bin/ and lib/ directories.
mkdir -p bin
chmod o+rwx bin
mkdir -p lib
chmod o+rwx lib

for arch in "$@"; do
  config=$(arch_to_config $arch)
  echo "Building $config, os=$os, arch=$arch..."
  # Build cockroach, workload and geos libs.
  bazel build --config $config --config ci -c opt --config force_build_cdeps \
        //pkg/cmd/cockroach //pkg/cmd/workload \
        //c-deps:libgeos
  BAZEL_BIN=$(bazel info bazel-bin --config $config --config ci -c opt)

  # Build cockroach-short with assertions enabled.
  bazel build --config $config --config ci -c opt //pkg/cmd/cockroach-short --crdb_test
  # Copy the binaries.
  cp $BAZEL_BIN/pkg/cmd/cockroach/cockroach_/cockroach bin/cockroach.$os-$arch
  cp $BAZEL_BIN/pkg/cmd/workload/workload_/workload    bin/workload.$os-$arch
  # N.B. "-ea" suffix stands for enabled-assertions.
  cp $BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short bin/cockroach-ea.$os-$arch
  # Make it writable to simplify cleanup and copying (e.g., scp retry).
  chmod a+w bin/cockroach.$os-$arch bin/workload.$os-$arch bin/cockroach-ea.$os-$arch

  # Copy geos libs.
  cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos.so   lib/libgeos.$os-$arch.so
  cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos_c.so lib/libgeos_c.$os-$arch.so
  # Make it writable to simplify cleanup and copying (e.g., scp retry).
  chmod a+w lib/libgeos.$os-$arch.so lib/libgeos_c.$os-$arch.so
done

# Build roachtest itself, for the host architecture.
# We also build geos libs; roachtest runner may require them locally.
config=$(arch_to_config $host_arch)
echo "Building roachtest and libgeos ($config, os=$os, arch=$host_arch)..."

bazel build --config $config --config ci  -c opt //pkg/cmd/roachtest
bazel build --config $config --config ci -c opt --config force_build_cdeps \
      //c-deps:libgeos

BAZEL_BIN=$(bazel info bazel-bin --config $config --config ci -c opt)
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin/roachtest
# N.B. geos does not support the architecture suffix (see getLibraryExt() in
# geos.go).
cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos.so   lib/libgeos.so
cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos_c.so lib/libgeos_c.so

# Make files writable to simplify cleanup and copying (e.g., scp retry).
chmod a+w bin/roachtest lib/libgeos.so lib/libgeos_c.so

ls -l bin
ls -l lib
