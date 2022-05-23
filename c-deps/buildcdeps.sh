#!/usr/bin/env bash

set -euxo pipefail

CONFIGS="linux linuxarm macos macosarm windows"

# Usage: bundle config directory
bundle() {
    filename=/artifacts/$(basename $2).$1.tar.gz
    tar -hczf $filename -C $2 .
}

for CONFIG in $CONFIGS; do
    TARGETS="libgeos libproj libjemalloc"
    if [[ $CONFIG == linux* ]]; then
        TARGETS="$TARGETS libkrb5"
    fi
    bazel clean
    bazel build --config ci --config cross$CONFIG --//build/toolchains:prebuild_cdeps_flag $(echo "$TARGETS" | python3 -c 'import sys; input = sys.stdin.read().strip(); print(" ".join("//c-deps:{}_foreign".format(w) for w in input.split(" ")))')
    BAZEL_BIN=$(bazel info bazel-bin --config ci --config cross$CONFIG)
    for TARGET in $TARGETS; do
        bundle $CONFIG $BAZEL_BIN/c-deps/${TARGET}_foreign
    done
done

for FILE in $(find /artifacts -name '*.tar.gz' | sort); do
    shasum -a 256 $FILE
done
