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
import { createSelector } from "reselect";
import { RouteComponentProps, withRouter } from "react-router-dom";
import {
  refreshNodes,
  refreshTxns,
  refreshTxnInsights,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { appNamesAttr, txnFingerprintIdAttr, unset } from "src/util/constants";
import { getMatchParamByName, queryByName } from "src/util/query";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import {
  reqSortSetting,
  selectData,
  selectLastError,
  limitSetting,
} from "src/views/transactions/transactionsPage";
import {
  TransactionDetailsStateProps,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
  TransactionDetails,
} from "@cockroachlabs/cluster-ui";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectTxnInsightsByFingerprint } from "src/views/insights/insightsSelectors";
import { selectHasAdminRole } from "src/redux/user";

export const selectTransaction = createSelector(
  (state: AdminUIState) => state.cachedData.transactions,
  (_state: AdminUIState, props: RouteComponentProps) => props,
  (transactionState, props) => {
    const transactions = transactionState.data?.transactions;
    if (!transactions) {
      return {
        isLoading: transactionState.inFlight,
        transaction: null,
        lastUpdated: null,
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
      lastUpdated: transactionState?.setAt?.utc(),
      isValid: transactionState.valid,
    };
  },
);

export default withRouter(
  connect<TransactionDetailsStateProps, TransactionDetailsDispatchProps>(
    (
      state: AdminUIState,
      props: TransactionDetailsProps,
    ): TransactionDetailsStateProps => {
      const { isLoading, transaction, lastUpdated, isValid } =
        selectTransaction(state, props);
      return {
        timeScale: selectTimeScale(state),
        error: selectLastError(state),
        isTenant: false,
        nodeRegions: nodeRegionsByIDSelector(state),
        statements: selectData(state)?.statements,
        transaction: transaction,
        transactionFingerprintId: getMatchParamByName(
          props.match,
          txnFingerprintIdAttr,
        ),
        isLoading: isLoading,
        lastUpdated: lastUpdated,
        transactionInsights: selectTxnInsightsByFingerprint(state, props),
        hasAdminRole: selectHasAdminRole(state),
        isDataValid: isValid,
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
      };
    },
    {
      refreshData: refreshTxns,
      refreshNodes,
      refreshUserSQLRoles,
      onTimeScaleChange: setGlobalTimeScaleAction,
      refreshTransactionInsights: refreshTxnInsights,
    },
  )(TransactionDetails),
);
