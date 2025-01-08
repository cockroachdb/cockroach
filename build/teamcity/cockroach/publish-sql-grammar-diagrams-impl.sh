#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

bazel build //pkg/cmd/bazci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci -- build \
		       //docs/generated/sql/bnf //docs/generated/sql/bnf:svg
