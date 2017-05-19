#!/usr/bin/env bash

set -euxo pipefail

build/builder.sh make build TYPE=release-linux-gnu
mkdir -p artifacts
mv cockroach-linux-2.6.32-gnu-amd64 artifacts/cockroach
