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
import { refreshStatements, refreshUserSQLRoles } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { txnFingerprintIdAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import {
  selectData,
  selectLastError,
} from "src/views/transactions/transactionsPage";
import { statementsTimeScaleLocalSetting } from "src/redux/statementsTimeScale";
import {
  TransactionDetailsStateProps,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
  TransactionDetails,
} from "@cockroachlabs/cluster-ui";
import { setCombinedStatementsTimeScaleAction } from "src/redux/statements";

export const selectTransaction = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (_state: AdminUIState, props: RouteComponentProps) => props,
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

export default withRouter(
  connect<TransactionDetailsStateProps, TransactionDetailsDispatchProps>(
    (
      state: AdminUIState,
      props: TransactionDetailsProps,
    ): TransactionDetailsStateProps => {
      const { isLoading, transaction } = selectTransaction(state, props);
      return {
        timeScale: statementsTimeScaleLocalSetting.selector(state),
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
      };
    },
    {
      refreshData: refreshStatements,
      refreshUserSQLRoles,
      onTimeScaleChange: setCombinedStatementsTimeScaleAction,
    },
  )(TransactionDetails),
);
