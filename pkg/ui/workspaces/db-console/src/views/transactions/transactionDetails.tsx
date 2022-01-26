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
import { aggregatedTsAttr, txnFingerprintIdAttr } from "src/util/constants";
import { TimestampToString } from "src/util/convert";
import { getMatchParamByName } from "src/util/query";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import {
  selectData,
  selectDateRange,
  selectLastError,
} from "src/views/transactions/transactionsPage";
import {
  TransactionDetailsStateProps,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
  TransactionDetails,
} from "@cockroachlabs/cluster-ui";

export const selectTransaction = createSelector(
  (state: AdminUIState) => state.cachedData.statements,
  (_state: AdminUIState, props: RouteComponentProps) => props,
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

export default withRouter(
  connect<TransactionDetailsStateProps, TransactionDetailsDispatchProps>(
    (
      state: AdminUIState,
      props: TransactionDetailsProps,
    ): TransactionDetailsStateProps => {
      const transaction = selectTransaction(state, props);
      return {
        aggregatedTs: getMatchParamByName(props.match, aggregatedTsAttr),
        dateRange: selectDateRange(state),
        error: selectLastError(state),
        isTenant: false,
        nodeRegions: nodeRegionsByIDSelector(state),
        statements: selectData(state)?.statements,
        transaction: transaction,
        transactionFingerprintId: getMatchParamByName(
          props.match,
          txnFingerprintIdAttr,
        ),
      };
    },
    { refreshData: refreshStatements, refreshUserSQLRoles },
  )(TransactionDetails),
);
