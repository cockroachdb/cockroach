// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AppState } from "../reducers";

const uiConfigSelectors = createSelector(
  (state: AppState) => state.adminUI,
  state => state?.uiConfig,
);

export const enableSqlStatsResetSelector = createSelector(
  uiConfigSelectors,
  config => {
    if (config?.pages?.statements?.enableSqlStatsReset === undefined) {
      return true; // enable "reset sql stats" by default
    }
    return config?.pages?.statements?.enableSqlStatsReset;
  },
);
