#!/usr/bin/env sh

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
BIN_DIR=$(realpath $THIS_DIR/../../bin/aspect-bazel-lib-utils-$(date +%Y%m%d-%H%M%S))

PLATS=(darwin_amd64 darwin_arm64 linux_amd64 linux_arm64 linux_s390x windows_amd64)

mkdir -p $BIN_DIR

for PLAT in "${PLATS[@]}"; do
    bazel build --platforms=@io_bazel_rules_go//go/toolchain:$PLAT @aspect_bazel_lib//tools/copy_directory @aspect_bazel_lib//tools/copy_to_directory
    if [ $PLAT == windows_amd64 ]; then
       EXT=.exe
    else
       EXT=
    fi
    cp _bazel/bin/external/aspect_bazel_lib/tools/copy_directory/copy_directory_/copy_directory$EXT $BIN_DIR/copy_directory-$PLAT$EXT
    chmod a+w $BIN_DIR/copy_directory-$PLAT$EXT
    cp _bazel/bin/external/aspect_bazel_lib/tools/copy_to_directory/copy_to_directory_/copy_to_directory$EXT $BIN_DIR/copy_to_directory-$PLAT$EXT
    chmod a+w $BIN_DIR/copy_to_directory-$PLAT$EXT
done

set +x
shasum -a 256 $BIN_DIR/*
echo "Binaries in $BIN_DIR"
