// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { localStorageSelector } from "src/store/utils/selectors";
import { AppState } from "src/store/reducers";

import { selectStatementInsightDetailsCombiner } from "src/selectors/insightsCommon.selectors";
import { selectID } from "src/selectors/common";

export const selectStmtInsights = (state: AppState) =>
  state.adminUI.stmtInsights?.data;

export const selectStmtInsightsError = (state: AppState) =>
  state.adminUI.stmtInsights?.lastError;

export const selectStmtInsightDetails = createSelector(
  selectStmtInsights,
  selectID,
  selectStatementInsightDetailsCombiner,
);

export const selectColumns = createSelector(
  localStorageSelector,
  localStorage =>
    localStorage["showColumns/StatementInsightsPage"]
      ? localStorage["showColumns/StatementInsightsPage"].split(",")
      : null,
);

export const selectStmtInsightsLoading = (state: AppState) =>
  !state.adminUI.stmtInsights?.valid || state.adminUI.stmtInsights?.inFlight;
