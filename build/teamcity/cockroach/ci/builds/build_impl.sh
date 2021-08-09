#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: build_impl.sh CONFIG'
    exit 1
fi

CONFIG="$1"

# Only build docs on Linux.
DOC_TARGETS=
if [ "$CONFIG" == "crosslinux" ]
then
   DOC_TARGETS="//docs/generated:gen-logging-md //docs/generated:gen-logsinks-md //docs/generated:gen-eventlog-md //docs/generated:gen-logformats-md //docs/generated/settings:settings //docs/generated/settings:settings_for_tenants //docs/generated/sql //docs/generated/sql/bnf"
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$CONFIG" \
		       build //pkg/cmd/cockroach-short //c-deps:libgeos $DOC_TARGETS
