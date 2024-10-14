// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AdminUIState } from "src/redux/state";

export function selectEnterpriseEnabled(state: AdminUIState) {
  return (
    state.cachedData.cluster.valid &&
    state.cachedData.cluster.data.enterprise_enabled
  );
}
