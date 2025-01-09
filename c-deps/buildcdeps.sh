#!/usr/bin/env bash

set -euxo pipefail

CONFIGS="linux linuxarm macos macosarm windows"

# Usage: bundle config directory
bundle() {
    filename=/artifacts/$(basename $2).$1.tar.gz
    tar -hczf $filename -C $2 .
}

for CONFIG in $CONFIGS; do
    TARGETS="libproj libjemalloc"
    if [[ $CONFIG != windows ]]; then
        TARGETS="$TARGETS libgeos"
    fi
    if [[ $CONFIG == linux* ]]; then
        TARGETS="$TARGETS libkrb5"
    fi
    bazel clean
    bazel build --config cross$CONFIG --//build/toolchains:prebuild_cdeps_flag $(echo "$TARGETS" | python3 -c 'import sys; input = sys.stdin.read().strip(); print(" ".join("//c-deps:{}_foreign".format(w) for w in input.split(" ")))')
    BAZEL_BIN=$(bazel info bazel-bin --config cross$CONFIG)
    for TARGET in $TARGETS; do
        # verify jemalloc was configured without madv_free
        if [[ $TARGET == libjemalloc ]]; then
            JEMALLOC_MADV_FREE_ENABLED=$((grep -E "^je_cv_madv_free=no$" $BAZEL_BIN/c-deps/${TARGET}_foreign_foreign_cc/Configure.log | awk -F"=" '{print $2}') || true)
            if [[ "$JEMALLOC_MADV_FREE_ENABLED" != "no" ]]; then
                echo "NOTE: using MADV_FREE with jemalloc can lead to surprising results; see https://github.com/cockroachdb/cockroach/issues/83790"
                exit 1
            fi
        fi
        bundle $CONFIG $BAZEL_BIN/c-deps/${TARGET}_foreign
    done
done

for FILE in $(find /artifacts -name '*.tar.gz' | sort); do
    shasum -a 256 $FILE
done
