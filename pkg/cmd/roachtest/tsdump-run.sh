#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

c=local-tsdump

if [ $# -eq 0 ]; then
  cat <<EOF
Host tsdump on local cluster $c.
Usage:

$0 /path/to/cockroach  # use local cockroach binary
$0 release vNN.YY.ZZ   # download specified release
$0 cockroach [SHA]     # download specified master SHA
EOF
  exit 1
fi

roachprod destroy local-tsdump &> /dev/null || true
roachprod create -n 1 $c

if [ -f "$1" ]; then
  roachprod put $c "$1" ./cockroach
else
  roachprod stage $c "$@"
fi

roachprod start --insecure --env COCKROACH_DEBUG_TS_IMPORT_FILE="${PWD}/tsdump.gob" $c

roachprod adminurl --insecure $c:1

trap "roachprod destroy $c" EXIT
echo "Running; hit CTRL-C to destroy the cluster"
while true; do sleep 86400; done
