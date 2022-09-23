#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- build --config ci \
				   //docs/generated/sql/bnf //docs/generated/sql/bnf:svg
