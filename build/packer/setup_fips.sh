#!/bin/bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail
ua enable fips --assume-yes
apt-get install -y dpkg-repack
