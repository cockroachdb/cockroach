// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  TransactionDetailsStateProps,
  TransactionDetailsDispatchProps,
  TransactionDetails,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps } from "react-router";
import { withRouter } from "react-router-dom";

import {
  refreshNodes,
  refreshTxns,
  refreshTxnInsights,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectHasAdminRole } from "src/redux/user";
import { txnFingerprintIdAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { selectTxnInsightsByFingerprint } from "src/views/insights/insightsSelectors";
import {
  reqSortSetting,
  limitSetting,
  selectTxns,
  requestTimeLocalSetting,
} from "src/views/transactions/transactionsPage";

export default withRouter(
  connect<
    TransactionDetailsStateProps,
    TransactionDetailsDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    (
      state: AdminUIState,
      props: RouteComponentProps,
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
