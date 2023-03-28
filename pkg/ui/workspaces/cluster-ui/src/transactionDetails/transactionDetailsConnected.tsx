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
import { actions as txnStatsActions } from "src/store/transactionStats";
import { TxnInsightsRequest } from "../api";
import {
  actions as transactionInsights,
  selectTxnInsightsByFingerprint,
} from "src/store/insights/transactionInsights";
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
  selectHasAdminRole,
} from "../store/uiConfig";
import { nodeRegionsByIDSelector } from "../store/nodes";
import {
  selectTimeScale,
  selectTxnsPageLimit,
  selectTxnsPageReqSort,
} from "../store/utils/selectors";
import { StatementsRequest } from "src/api/statementsApi";
import {
  txnFingerprintIdAttr,
  getMatchParamByName,
  queryByName,
  appNamesAttr,
  unset,
} from "../util";
import { TimeScale } from "../timeScaleDropdown";
import { actions as analyticsActions } from "../store/analytics";

export const selectTransaction = createSelector(
  (state: AppState) => state.adminUI?.transactions,
  (_state: AppState, props: RouteComponentProps) => props,
  (transactionState, props) => {
    const transactions = transactionState.data?.transactions;
    if (!transactions) {
      return {
        isLoading: transactionState.inFlight,
        transaction: null,
        isValid: transactionState.valid,
      };
    }

    const apps = queryByName(props.location, appNamesAttr)
      ?.split(",")
      .map(s => s.trim());

    const txnFingerprintId = getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    );

    const transaction = transactions.find(
      txn =>
        txn.stats_data.transaction_fingerprint_id.toString() ===
          txnFingerprintId &&
        (apps?.length ? apps.includes(txn.stats_data.app ?? unset) : true),
    );

    return {
      isLoading: transactionState.inFlight,
      transaction: transaction,
      lastUpdated: transactionState.lastUpdated,
      isValid: transactionState.valid,
    };
  },
);

const mapStateToProps = (
  state: AppState,
  props: TransactionDetailsProps,
): TransactionDetailsStateProps => {
  const { isLoading, transaction, lastUpdated, isValid } = selectTransaction(
    state,
    props,
  );
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
    lastUpdated: lastUpdated,
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    transactionInsights: selectTxnInsightsByFingerprint(state, props),
    hasAdminRole: selectHasAdminRole(state),
    isDataValid: isValid,
    limit: selectTxnsPageLimit(state),
    reqSortSetting: selectTxnsPageReqSort(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionDetailsDispatchProps => ({
  refreshData: (req?: StatementsRequest) =>
    dispatch(txnStatsActions.refresh(req)),
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Transaction Details",
        value: ts.key,
      }),
    );
  },
  refreshTransactionInsights: (req: TxnInsightsRequest) => {
    dispatch(transactionInsights.refresh(req));
  },
});

export const TransactionDetailsPageConnected = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(TransactionDetails),
);
