#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_UNVENDORED=1
exec ./build/builder.sh build/checkdeps.sh
