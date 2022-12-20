// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import {
  TransactionInsightDetails,
  TransactionInsightDetailsStateProps,
  TransactionInsightDetailsDispatchProps,
} from "./transactionInsightDetails";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AppState } from "src/store";
import {
  selectTransactionInsightDetails,
  selectTransactionInsightDetailsError,
  actions,
} from "src/store/insightDetails/transactionInsightDetails";
import { TimeScale } from "../../timeScaleDropdown";
import { actions as sqlStatsActions } from "../../store/sqlStats";
import { Dispatch } from "redux";
import { TxnContentionReq } from "src/api";
import { selectTimeScale } from "../../store/utils/selectors";

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
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionInsightDetailsDispatchProps => ({
  refreshTransactionInsightDetails: (req: TxnContentionReq) => {
    dispatch(actions.refresh(req));
  },
  setTimeScale: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
  },
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
