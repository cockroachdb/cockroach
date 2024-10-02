// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { StatementsRequest } from "src/api/statementsApi";
import { AppState, uiConfigActions } from "src/store";
import {
  actions as transactionInsights,
  selectTxnInsightsByFingerprint,
} from "src/store/insights/transactionInsights";
import { actions as localStorageActions } from "src/store/localStorage";
import { actions as sqlStatsActions } from "src/store/sqlStats";
import { actions as txnStatsActions } from "src/store/transactionStats";
import { selectRequestTime } from "src/transactionsPage/transactionsPage.selectors";

import { TxnInsightsRequest } from "../api";
import { actions as analyticsActions } from "../store/analytics";
import {
  nodeRegionsByIDSelector,
  actions as nodesActions,
} from "../store/nodes";
import {
  selectIsTenant,
  selectHasViewActivityRedactedRole,
  selectHasAdminRole,
} from "../store/uiConfig";
import {
  selectTimeScale,
  selectTxnsPageLimit,
  selectTxnsPageReqSort,
} from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";
import { txnFingerprintIdAttr, getMatchParamByName } from "../util";

import {
  TransactionDetails,
  TransactionDetailsDispatchProps,
  TransactionDetailsProps,
  TransactionDetailsStateProps,
} from "./transactionDetails";

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

export const TransactionDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(TransactionDetails),
);
