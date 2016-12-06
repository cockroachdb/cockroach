#!/usr/bin/env bash
set -exuo pipefail

exec ./build/builder.sh build/checkdeps.sh
