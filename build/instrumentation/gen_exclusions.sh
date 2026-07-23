#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

echo "# Vendor Exclusions"
if [ -z "$VENDOR_EXCLUDE_ALL" ]; then
    find vendor -mindepth 1 -maxdepth 1 -type d ! -path "vendor/github.com" ! -path "vendor/go.etcd.io"
    find vendor/github.com -mindepth 2 -maxdepth 2 -type d ! -path "vendor/github.com/cockroachdb/*"
else
    echo "vendor"
fi

echo
echo "# Exclusions for cmds other than workload, cockroach.*"
find pkg/cmd -mindepth 1 -maxdepth 1 -type d ! -name "cockroach*" ! -name "workload"
echo
cat ${SCRIPT_DIR}/default_exclusions.txt
echo
