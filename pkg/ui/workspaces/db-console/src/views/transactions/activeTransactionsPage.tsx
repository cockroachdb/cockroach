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
  activeTransactionsFromSessions,
  ActiveTransactionsView,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { SessionsResponseMessage } from "src/util/api";
import { refreshSessions } from "src/redux/apiReducers";

const selectActiveTransactions = createSelector(
  (state: AdminUIState) => state.cachedData.sessions,
  (
    state?: CachedDataReducerState<SessionsResponseMessage>,
  ): ActiveTransaction[] => {
    if (state.data == null) return [];
    return activeTransactionsFromSessions(state.data);
  },
);

const transactionsColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export default withRouter(
  connect<
    ActiveTransactionsViewStateProps,
    ActiveTransactionsViewDispatchProps,
    RouteComponentProps
  >(
    (state: AdminUIState) => ({
      selectedColumns: transactionsColumnsLocalSetting.selectorToArray(state),
      transactions: selectActiveTransactions(state),
      fetchError: state.cachedData?.sessions.lastError,
    }),
    {
      onColumnsSelect: (columns: string[]) => {
        transactionsColumnsLocalSetting.set(
          columns.length === 0 ? " " : columns.join(","),
        );
      },
      refreshSessions,
    },
  )(ActiveTransactionsView),
);
