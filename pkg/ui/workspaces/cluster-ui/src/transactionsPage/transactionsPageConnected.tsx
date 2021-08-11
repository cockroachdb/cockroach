// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";
import { Moment } from "moment";

import { AppState } from "src/store";
import { actions as transactionsActions } from "src/store/transactions";
import { actions as resetSQLStatsActions } from "src/store/sqlStats";
import { actions as statementsActions } from "src/store/statements";
import { TransactionsPage } from "./transactionsPage";
import {
  TransactionsPageStateProps,
  TransactionsPageDispatchProps,
} from "./transactionsPage";
import {
  selectTransactionsData,
  selectTransactionsLastError,
} from "./transactionsPage.selectors";
import { selectIsTenant } from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectDateRange } from "src/statementsPage/statementsPage.selectors";
import { StatementsRequest } from "src/api/statementsApi";

export const TransactionsPageConnected = withRouter(
  connect<
    TransactionsPageStateProps,
    TransactionsPageDispatchProps,
    RouteComponentProps
  >(
    (state: AppState) => ({
      data: selectTransactionsData(state),
      nodeRegions: nodeRegionsByIDSelector(state),
      error: selectTransactionsLastError(state),
      isTenant: selectIsTenant(state),
      dateRange: selectIsTenant(state) ? null : selectDateRange(state),
    }),
    (dispatch: Dispatch) => ({
      refreshData: (req?: StatementsRequest) =>
        dispatch(transactionsActions.refresh(req)),
      resetSQLStats: () => dispatch(resetSQLStatsActions.request()),
      onDateRangeChange: (start: Moment, end: Moment) => {
        dispatch(
          statementsActions.updateDateRange({
            start: start.unix(),
            end: end.unix(),
          }),
        );
      },
    }),
  )(TransactionsPage),
);
