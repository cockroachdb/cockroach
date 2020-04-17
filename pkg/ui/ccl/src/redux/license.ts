// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import { AdminUIState } from "src/redux/state";

export function selectEnterpriseEnabled(state: AdminUIState) {
  return (
    state.cachedData.cluster.valid &&
    state.cachedData.cluster.data.enterprise_enabled
  );
}
