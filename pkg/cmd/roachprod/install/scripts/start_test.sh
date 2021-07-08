#!/usr/bin/env bash
#
# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.
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
