// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { createSelector } from "reselect";
import {
  InsightEventFilters,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";

export const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  InsightEventFilters
>("filters/InsightsPage", (state: AdminUIState) => state.localSettings, {
  app: defaultFilters.app,
});

export const sortSettingLocalSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>("sortSetting/InsightsPage", (state: AdminUIState) => state.localSettings, {
  ascending: false,
  columnTitle: "startTime",
});

export const selectInsights = createSelector(
  (state: AdminUIState) => state.cachedData,
  adminUiState => {
    if (!adminUiState.insights) return [];
    return adminUiState.insights.data;
  },
);
