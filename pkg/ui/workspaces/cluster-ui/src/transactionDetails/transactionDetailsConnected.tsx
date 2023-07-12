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
import { withRouter } from "react-router-dom";
import { Dispatch } from "redux";
import { actions as localStorageActions } from "src/store/localStorage";
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
import { txnFingerprintIdAttr, getMatchParamByName } from "../util";
import { TimeScale } from "../timeScaleDropdown";
import { actions as analyticsActions } from "../store/analytics";
import { selectRequestTime } from "src/transactionsPage/transactionsPage.selectors";

const mapStateToProps = (
  state: AppState,
  props: TransactionDetailsProps,
): TransactionDetailsStateProps => {
  return {
    timeScale: selectTimeScale(state),
    isTenant: selectIsTenant(state),
    nodeRegions: nodeRegionsByIDSelector(state),
    txnStatsResp: state?.adminUI?.transactions,
    transactionFingerprintId: getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    ),
    hasViewActivityRedactedRole: selectHasViewActivityRedactedRole(state),
    transactionInsights: selectTxnInsightsByFingerprint(state, props),
    hasAdminRole: selectHasAdminRole(state),
    limit: selectTxnsPageLimit(state),
    reqSortSetting: selectTxnsPageReqSort(state),
    requestTime: selectRequestTime(state),
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
  onRequestTimeChange: (t: moment.Moment) => {
    dispatch(
      localStorageActions.update({
        key: "requestTime/StatementsPage",
        value: t,
      }),
    );
  },
});

export const TransactionDetailsPageConnected = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(TransactionDetails),
);
