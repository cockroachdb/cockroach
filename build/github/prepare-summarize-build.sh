#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

THIS_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

bazel build //pkg/cmd/bazci/bazel-github-helper --config crosslinux --jobs 100 $($THIS_DIR/engflow-args.sh) --bes_keywords helper-binary
