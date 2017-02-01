#!/usr/bin/env bash
set -exuo pipefail

build_dir="$(dirname $0)"

exec "${build_dir}"/builder.sh "${build_dir}"/checkdeps.sh
