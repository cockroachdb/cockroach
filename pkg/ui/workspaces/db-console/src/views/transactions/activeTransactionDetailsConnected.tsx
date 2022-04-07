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
  ActiveTransaction,
  getActiveTransactionsFromSessions,
  ActiveTransactionDetails,
  ActiveTransactionDetailsStateProps,
  ActiveTransactionDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";
import { executionIdAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { refreshSessions } from "src/redux/apiReducers";

const selectActiveTransaction = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (_state: AdminUIState, props: RouteComponentProps) => props,
  (
    state: CachedDataReducerState<SessionsResponseMessage>,
    props: RouteComponentProps,
  ): ActiveTransaction | null => {
    const id = getMatchParamByName(props.match, executionIdAttr);
    if (state?.data?.sessions == null) return null;
    return getActiveTransactionsFromSessions(state.data, state.setAt).find(
      txn => txn.executionID === id,
    );
  },
);

export default withRouter(
  connect<
    ActiveTransactionDetailsStateProps,
    ActiveTransactionDetailsDispatchProps,
    RouteComponentProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      transaction: selectActiveTransaction(state, props),
      match: props.match,
    }),
    { refreshSessions },
  )(ActiveTransactionDetails),
);
