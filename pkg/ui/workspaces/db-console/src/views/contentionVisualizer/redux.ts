// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ContentionDebugStateProps } from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";
import { AdminUIState } from "oss/src/redux/state";

import { refreshTxnContentionEvents } from "src/redux/apiReducers";

const selectContentionEvents = createSelector(
  (state: AdminUIState) => state.cachedData.txnContentionEvents,
  txnContentionEvents => txnContentionEvents.data,
);

const selectContentionError = createSelector(
  (state: AdminUIState) => state.cachedData.txnContentionEvents,
  txnContentionEvents => txnContentionEvents.lastError,
);

export const mapStateToProps = (
  state: AdminUIState,
): ContentionDebugStateProps => ({
  contentionEvents: selectContentionEvents(state),
  contentionError: selectContentionError(state),
});

export const mapDispatchToProps = {
  refreshTxnContentionEvents,
};
