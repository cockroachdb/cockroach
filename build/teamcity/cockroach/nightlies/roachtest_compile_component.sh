#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

source $root/build/teamcity/util/roachtest_arch_util.sh

if [ "$#" -eq 0 ]; then
  echo "Builds components necessary for roachtests and stages them in bin/ and/or lib/."
  echo ""
  echo "Usage: $(basename $0) [--with-coverage] <os/arch/component>"
  echo "  where os is one of: linux"
  echo "        arch is one of: amd64, arm64, amd64-fips"
  echo "        component is one of: cockroach, cockroach-ea, workload, libgeos, roachtest"
  echo "  --with-coverage enables go code coverage instrumentation (only applies to cockroach binaries)"
  exit 1
fi

crdb_extra_flags=""
target=""

for arg in "$@"; do
  case "$arg" in
    --with-code-coverage)
      crdb_extra_flags="--collect_code_coverage --bazel_code_coverage"
      ;;
    *)
      if [ -n "$target" ]; then
        echo "Error: too many arguments"
        exit 1
      fi
      target=$arg
      ;;
  esac
done

# Split $target into $os/$arch/$component.
os=${target%%/*}
arch_and_comp=${target#*/}
arch=${arch_and_comp%%/*}
component=${arch_and_comp#*/}

if [ -z $os -o -z $arch -o -z $component ]; then
  echo "Invalid target: $target; syntax is os/arch/component"
  exit 1
fi

if [ "$os" != linux ]; then
  echo "Invalid os; only linux supported"
  exit 1
fi

config=$(arch_to_config $arch)

# Array of arguments to be passed to bazel for the component.
bazel_args=()
# Array of build artifacts. Each item has format "src:dest"; src is relative to
# the bazel-bin directory, dst is relative to cwd.
artifacts=()

case "$component" in
  cockroach)
    # Cockroach binary.
    bazel_args=(--config force_build_cdeps --norun_validations //pkg/cmd/cockroach $crdb_extra_flags)
    artifacts=("pkg/cmd/cockroach/cockroach_/cockroach:bin/cockroach.$os-$arch")
    ;;
  cockroach-ea)
    # Cockroach binary with enabled assertions (EA).
    bazel_args=(--config force_build_cdeps --norun_validations //pkg/cmd/cockroach --crdb_test $crdb_extra_flags)
    artifacts=("pkg/cmd/cockroach/cockroach_/cockroach:bin/cockroach-ea.$os-$arch")
    ;;
  workload)
    # Workload binary.
    bazel_args=(--config force_build_cdeps --norun_validations //pkg/cmd/workload)
    artifacts=("pkg/cmd/workload/workload_/workload:bin/workload.$os-$arch")
    ;;
  libgeos)
    # Geos libs.
    bazel_args=(--config force_build_cdeps //c-deps:libgeos)
    artifacts=(
      "c-deps/libgeos_foreign/lib/libgeos.so:lib/libgeos.$os-$arch.so"
      "c-deps/libgeos_foreign/lib/libgeos_c.so:lib/libgeos_c.$os-$arch.so"
    )
    ;;
  roachtest)
    # Roachtest binary.
    # N.B. We always compile the roachtest binary with crdb_test so the same serialization
    # on the wire is established with cockroach binaries built under crdb_test.
    # E.g. KVNemesisSeq is used only under crdb_test builds. If the cockroach binary is
    # built with crdb_test, it will expect this field to be sent by the roachtest runner.
    # Note that the opposite is not true, and a cockroach binary built without crdb_test
    # is still compatible with a roachtest binary built with it.
    bazel_args=(//pkg/cmd/roachtest --crdb_test)
    artifacts=("pkg/cmd/roachtest/roachtest_/roachtest:bin/roachtest.$os-$arch")
    ;;
  roachprod)
      # Roachprod binary.
      # This binary is built to support the logic behind `roachprod update`.
      # Hence, we do not need to add `--crdb_test` to the build args as we do
      # for `roachtest`. Adding the build flag causes the binary to log
      # metamorphic vars on each command invocation, which is not ideal from a
      # user experience perspective.
      bazel_args=(//pkg/cmd/roachprod)
      artifacts=("pkg/cmd/roachprod/roachprod_/roachprod:bin/roachprod.$os-$arch")
      ;;
  *)
    echo "Unknown component '$component'"
    exit 1
    ;;
esac

echo "Building $os/$arch/$component..."

bazel build --config $config -c opt "${bazel_args[@]}"
BAZEL_BIN=$(bazel info bazel-bin --config $config -c opt)
for artifact in "${artifacts[@]}"; do
  src=${artifact%%:*}
  dst=${artifact#*:}
  cp "$BAZEL_BIN/$src" "$dst"
  # Make files writable to simplify cleanup and copying (e.g., scp retry).
  chmod a+w "$dst"
done
