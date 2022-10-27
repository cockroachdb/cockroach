#!/usr/bin/env bash
#
# This script is run by the Pebble Nightly Crossversion Metamorphic - TeamCity
# build configuration.

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
pebbledir="$(dirname ${dir})/pebble"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel


mkdir -p bin
chmod o+rwx bin
mkdir -p $root/artifacts

go install github.com/cockroachdb/stress@master

cd "$(dirname ${dir})"
git clone "https://github.com/cockroachdb/pebble.git"
cd pebble
STRESS=1 ./scripts/run-crossversion-meta.sh "$@"
