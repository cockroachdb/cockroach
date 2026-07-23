#!/usr/bin/env bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
set -euo pipefail

rm bin/roachprod || true
make bin/roachprod
clus="${USER}-starttest"
for c in local "${clus}"; do
    bin/roachprod wipe "${c}"
    bin/roachprod stage "${c}" cockroach
  for i in 1 2; do
    bin/roachprod start "${c}"
    bin/roachprod monitor --oneshot "${c}"
    bin/roachprod stop "${c}"
    bin/roachprod ssh "${c}" "echo hello $i"
  done
done
