#!/usr/bin/env bash

set -euxo pipefail

THIS_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

bazel build //pkg/cmd/bazci/bazel-github-helper --config crosslinux --jobs 100 $($THIS_DIR/engflow-args.sh) --bes_keywords helper-binary
