#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

if ! which pprofme; then
	echo "pprofme missing, setup instructions: https://github.com/polarsignals/pprofme#install"
	exit 1
fi

pprofme upload "$@"
