// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState, uiConfigActions } from "src/store";
import { actions as nodesActions } from "../store/nodes";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import {
  TransactionDetails,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
  TransactionDetailsStateProps,
} from "./transactionDetails";
import {
  selectTransactionsData,
  selectTransactionsLastError,
} from "../transactionsPage/transactionsPage.selectors";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
} from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectTimeScale } from "../store/utils/selectors";
import { StatementsRequest } from "src/api/statementsApi";
import { txnFingerprintIdAttr, getMatchParamByName } from "../util";
import { TimeScale } from "../timeScaleDropdown";

export const selectTransaction = createSelector(
  (state: AppState) => state.adminUI.sqlStats,
  (_state: AppState, props: RouteComponentProps) => props,
  (transactionState, props) => {
    const transactions = transactionState.data?.transactions;
    if (!transactions) {
      return {
        isLoading: true,
        transaction: null,
      };
    }
    const txnFingerprintId = getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    );

    const transaction = transactions.filter(
      txn =>
        txn.stats_data.transaction_fingerprint_id.toString() ==
        txnFingerprintId,
    )[0];
    return {
      isLoading: false,
      transaction: transaction,
    };
  },
);

const mapStateToProps = (
  state: AppState,
  props: TransactionDetailsProps,
): TransactionDetailsStateProps => {
  const { isLoading, transaction } = selectTransaction(state, props);
  return {
    timeScale: selectTimeScale(state),
    error: selectTransactionsLastError(state),
    isTenant: selectIsTenant(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    statements: selectTransactionsData(state)?.statements,
    transaction,
    transactionFingerprintId: getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    ),
    isLoading: isLoading,
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionDetailsDispatchProps => ({
  refreshData: (req?: StatementsRequest) =>
    dispatch(sqlStatsActions.refresh(req)),
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
});

export const TransactionDetailsPageConnected = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(TransactionDetails),
);
