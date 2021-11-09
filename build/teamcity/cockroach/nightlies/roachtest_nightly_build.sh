#!/usr/bin/env bash
set -exuo pipefail

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci build \
				   --config crosslinux --config ci --config with_ui \
				   //pkg/cmd/cockroach //pkg/cmd/workload //pkg/cmd/roachtest //pkg/cmd/roachprod
