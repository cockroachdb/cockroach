#!/usr/bin/env bash

# Determine host cpu architecture, which we'll need for libgeos, below.
if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export host_arch=arm64
else
  export host_arch=amd64
fi

echo "Host architecture: $host_arch"

# Builds all bits needed for roachtests, stages them in bin/ and lib/.
cross_builds=(crosslinux crosslinuxarm crosslinuxfips)

# Prepare the bin/ and lib/ directories.
mkdir -p bin
chmod o+rwx bin
mkdir -p lib
chmod o+rwx lib

for platform in "${cross_builds[@]}"; do
  if [[ $platform == crosslinux ]]; then
    os=linux
    arch=amd64
  elif [[ $platform == crosslinuxarm ]]; then
    os=linux
    arch=arm64
  elif [[ $platform == crosslinuxfips ]]; then
    os=linux
    arch=amd64-fips
  else
    echo "unknown or unsupported platform $platform"
    exit 1
  fi

  echo "Building $platform, os=$os, arch=$arch..."
  # Build cockroach, workload and geos libs.
  bazel build --config $platform --config ci -c opt --config force_build_cdeps \
        //pkg/cmd/cockroach //pkg/cmd/workload \
        //c-deps:libgeos
  BAZEL_BIN=$(bazel info bazel-bin --config $platform --config ci -c opt)

  # N.B. roachtest is built once, for the host architecture.
  if [[ $os == "linux" && $arch == $host_arch ]]; then
    bazel build --config $platform --config ci  -c opt //pkg/cmd/roachtest

    cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin/roachtest
    # Make it writable to simplify cleanup and copying (e.g., scp retry).
    chmod a+w bin/roachtest
  fi
  # Build cockroach-short with assertions enabled.
  bazel build --config $platform --config ci -c opt //pkg/cmd/cockroach-short --crdb_test
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

  if [[ $os == "linux" && $arch == $host_arch ]]; then
    # Copy geos libs for the host architecture; e.g., roachtest runner may require them locally.
    # N.B. we must drop the architecture suffix since geos doesn't support it (see getLibraryExt() in geos.go).
    cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos.so   lib/libgeos.so
    cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos_c.so lib/libgeos_c.so
    # Make it writable to simplify cleanup and copying (e.g., scp retry).
    chmod a+w lib/libgeos.so lib/libgeos_c.so
  fi

done

ls -l bin
ls -l lib

