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
import { actions as sqlStatsActions } from "src/store/sqlStats";
import {
  TransactionDetails,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
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
import { selectTimeScale } from "src/statementsPage/statementsPage.selectors";
import { StatementsRequest } from "src/api/statementsApi";
import {
  aggregatedTsAttr,
  txnFingerprintIdAttr,
  getMatchParamByName,
  TimestampToString,
} from "../util";

export const selectTransaction = createSelector(
  (state: AppState) => state.adminUI.sqlStats,
  (_state: AppState, props: RouteComponentProps) => props,
  (transactionState, props) => {
    const transactions = transactionState.data?.transactions;
    if (!transactions) {
      return null;
    }
    const aggregatedTs = getMatchParamByName(props.match, aggregatedTsAttr);
    const txnFingerprintId = getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    );

    return transactions
      .filter(
        txn =>
          txn.stats_data.transaction_fingerprint_id.toString() ==
          txnFingerprintId,
      )
      .filter(
        txn => TimestampToString(txn.stats_data.aggregated_ts) == aggregatedTs,
      )[0];
  },
);

const mapStateToProps = (state: AppState, props: TransactionDetailsProps) => {
  return {
    aggregatedTs: getMatchParamByName(props.match, aggregatedTsAttr),
    timeScale: selectTimeScale(state),
    error: selectTransactionsLastError(state),
    isTenant: selectIsTenant(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    statements: selectTransactionsData(state)?.statements,
    transaction: selectTransaction(state, props),
    transactionFingerprintId: getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    ),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionDetailsDispatchProps => ({
  refreshData: (req?: StatementsRequest) =>
    dispatch(sqlStatsActions.refresh(req)),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const TransactionDetailsPageConnected = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(TransactionDetails),
);
