#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -o pipefail

TPCC_DB=cct_tpcc
export ROACHPROD_GCE_DEFAULT_PROJECT=cockroach-drt
export ROACHPROD_DNS="drt.crdb.io"
./roachprod sync
sleep 20
PGURLS=$(./roachprod pgurl drt-scale:1-150 | sed s/\'//g)

./cockroach workload init tpcc \
  --warehouses 3000 \
  --db $TPCC_DB \
  --secure \
  --families \
  $PGURLS
