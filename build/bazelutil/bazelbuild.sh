#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: bazelbuild.sh CONFIG'
    exit 1
fi

CONFIG="$1"

DOC_TARGETS=
if [ "$CONFIG" == "crosslinux" ]
then
   DOC_TARGETS="//docs/generated:gen-logging-md //docs/generated/settings:settings //docs/generated/settings:settings_for_tenants //docs/generated/sql"
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$CONFIG" \
		       build //pkg/cmd/cockroach-short //c-deps:libgeos $DOC_TARGETS
