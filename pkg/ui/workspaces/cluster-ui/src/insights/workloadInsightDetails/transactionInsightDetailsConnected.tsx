// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { TxnInsightDetailsRequest } from "src/api";
import { AppState, uiConfigActions } from "src/store";
import {
  selectTransactionInsightDetails,
  selectTransactionInsightDetailsError,
  actions,
  selectTransactionInsightDetailsMaxSizeReached,
} from "src/store/insightDetails/transactionInsightDetails";

import { actions as analyticsActions } from "../../store/analytics";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectHasAdminRole } from "../../store/uiConfig";
import { selectTimeScale } from "../../store/utils/selectors";
import { TimeScale } from "../../timeScaleDropdown";

import {
  TransactionInsightDetails,
  TransactionInsightDetailsStateProps,
  TransactionInsightDetailsDispatchProps,
} from "./transactionInsightDetails";

const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): TransactionInsightDetailsStateProps => {
  const insightDetails = selectTransactionInsightDetails(state, props);
  const insightError = selectTransactionInsightDetailsError(state, props);
  return {
    insightDetails: insightDetails,
    insightError: insightError,
    timeScale: selectTimeScale(state),
    hasAdminRole: selectHasAdminRole(state),
    maxSizeApiReached: selectTransactionInsightDetailsMaxSizeReached(
      state,
      props,
    ),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionInsightDetailsDispatchProps => ({
  refreshTransactionInsightDetails: (req: TxnInsightDetailsRequest) => {
    dispatch(actions.refresh(req));
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Transaction Insight Details",
        value: ts.key,
      }),
    );
  },
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const TransactionInsightDetailsConnected = withRouter(
  connect<
    TransactionInsightDetailsStateProps,
    TransactionInsightDetailsDispatchProps,
    RouteComponentProps,
    AppState
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(TransactionInsightDetails),
);
