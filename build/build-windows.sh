#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")"/..

# TODO(tamird, #14673): make CCL compile on Windows.
build/builder.sh make buildoss TYPE=release-windows
