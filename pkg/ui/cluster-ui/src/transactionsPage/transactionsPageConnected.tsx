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

import { AppState } from "src/store";
import { actions as transactionsActions } from "src/store/transactions";
import { actions as resetSQLStatsActions } from "src/store/sqlStats";
import { TransactionsPage } from "./transactionsPage";
import {
  TransactionsPageStateProps,
  TransactionsPageDispatchProps,
} from "./transactionsPage";
import {
  selectTransactionsData,
  selectTransactionsLastError,
} from "./transactionsPage.selectors";
import { nodeRegionsByIDSelector } from "../store/nodes";

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
    }),
    (dispatch: Dispatch) => ({
      refreshData: () => dispatch(transactionsActions.refresh()),
      resetSQLStats: () => dispatch(resetSQLStatsActions.request()),
    }),
  )(TransactionsPage),
);
