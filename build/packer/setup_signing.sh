#!/bin/bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

# Install rustup, because the current rustc version is too old to build apple-codesign.
apt-get install --yes rustup
apt-get clean
rustup default stable
cargo install --root=/usr/local apple-codesign@0.29.0
