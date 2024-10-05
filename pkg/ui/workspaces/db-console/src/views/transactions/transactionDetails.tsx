// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter } from "react-router-dom";
import {
  refreshNodes,
  refreshTxns,
  refreshTxnInsights,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import {
  reqSortSetting,
  limitSetting,
  selectTxns,
  requestTimeLocalSetting,
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
import { getMatchParamByName } from "src/util/query";
import { txnFingerprintIdAttr } from "src/util/constants";

export default withRouter(
  connect<TransactionDetailsStateProps, TransactionDetailsDispatchProps>(
    (
      state: AdminUIState,
      props: TransactionDetailsProps,
    ): TransactionDetailsStateProps => {
      return {
        timeScale: selectTimeScale(state),
        isTenant: false,
        nodeRegions: nodeRegionsByIDSelector(state),
        txnStatsResp: selectTxns(state),
        transactionFingerprintId: getMatchParamByName(
          props.match,
          txnFingerprintIdAttr,
        ),
        transactionInsights: selectTxnInsightsByFingerprint(state, props),
        hasAdminRole: selectHasAdminRole(state),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
      };
    },
    {
      refreshData: refreshTxns,
      refreshNodes,
      refreshUserSQLRoles,
      onTimeScaleChange: setGlobalTimeScaleAction,
      refreshTransactionInsights: refreshTxnInsights,
      onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
    },
  )(TransactionDetails),
);
