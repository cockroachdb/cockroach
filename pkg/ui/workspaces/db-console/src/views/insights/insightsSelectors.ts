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
  defaultFilters,
  SortSetting,
  InsightEventFilters,
  api,
} from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { getMatchParamByName } from "src/util/query";

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

export const selectInsightDetails = createSelector(
  [
    (state: AdminUIState) => state.cachedData.insightDetails,
    (_state: AdminUIState, props: RouteComponentProps) => props,
  ],
  (insight, props): CachedDataReducerState<api.InsightEventDetailsResponse> => {
    const insightId = getMatchParamByName(props.match, "id");
    if (!insight) {
      return null;
    }
    return insight[insightId];
  },
);
