// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { createSelector } from "reselect";
import { withRouter } from "react-router-dom";
import {
  refreshStatements,
} from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { StatementsResponseMessage } from "src/util/api";

import { TimestampToMoment } from "src/util/convert";
import { PrintTime } from "src/views/reports/containers/range/print";

import { TransactionsPage } from "@cockroachlabs/admin-ui-components";

// selectStatements returns the array of AggregateStatistics to show on the
// TransactionsPage, based on if the appAttr route parameter is set.
export const selectData = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    return state.data || null;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (state: CachedDataReducerState<StatementsResponseMessage>) => {
    if (!state.data) {
      return "unknown";
    }

    return PrintTime(TimestampToMoment(state.data.last_reset));
  },
);

// tslint:disable-next-line:variable-name
const TransactionsPageConnected = withRouter(connect(
  (state: AdminUIState) => ({
    data: selectData(state),
    statementsError: state.cachedData.statements.lastError,
    lastReset: selectLastReset(state),
  }),
  {
    refreshData: refreshStatements,
  },
)(TransactionsPage));

export default TransactionsPageConnected;
