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

import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { txnFingerprintIdAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import {
  reqSortSetting,
  limitSetting,
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
        transactionFingerprintId: getMatchParamByName(
          props.match,
          txnFingerprintIdAttr,
        ),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
      };
    },
    {
      onTimeScaleChange: setGlobalTimeScaleAction,
      onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
    },
  )(TransactionDetails),
);
