// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState, uiConfigActions } from "src/store";
import {
  selectTransactionInsightDetails,
  selectTransactionInsightDetailsError,
  actions,
  selectTransactionInsightDetailsMaxSizeReached,
} from "src/store/insightDetails/transactionInsightDetails";
import { TxnInsightDetailsRequest } from "src/api";

import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { selectTimeScale } from "../../store/utils/selectors";
import { selectHasAdminRole } from "../../store/uiConfig";
import { actions as analyticsActions } from "../../store/analytics";

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
    RouteComponentProps
  >(
    mapStateToProps,
    mapDispatchToProps,
  )(TransactionInsightDetails),
);
