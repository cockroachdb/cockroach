// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import { AppState } from "src/store";
import { actions as localStorageActions } from "src/store/localStorage";
import { selectRequestTime } from "src/transactionsPage/transactionsPage.selectors";

import { actions as analyticsActions } from "../store/analytics";
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
    transactionFingerprintId: getMatchParamByName(
      props.match,
      txnFingerprintIdAttr,
    ),
    limit: selectTxnsPageLimit(state),
    reqSortSetting: selectTxnsPageReqSort(state),
    requestTime: selectRequestTime(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): TransactionDetailsDispatchProps => ({
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      localStorageActions.updateTimeScale({
        value: ts,
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
