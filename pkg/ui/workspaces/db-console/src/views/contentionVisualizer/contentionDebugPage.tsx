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
import {
  ContentionDebugPage,
  ContentionDebugStateProps,
  ContentionDebugDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";
import { AdminUIState } from "src/redux/state";
import { refreshTxnContentionEvents } from "src/redux/apiReducers";

const selectContentionEvents = createSelector(
  (state: AdminUIState) => state.cachedData.txnContentionEvents,
  txnContentionEvents => txnContentionEvents,
);

const selectContentionEventsData = createSelector(
  selectContentionEvents,
  events => {
    return events.data;
  },
);

const selectContentionEventsError = createSelector(
  selectContentionEvents,
  events => events.lastError,
);

const mapStateToProps = (state: AdminUIState): ContentionDebugStateProps => {
  const events = selectContentionEventsData(state);
  const error = selectContentionEventsError(state);
  return {
    contentionEvents: events,
    contentionError: error,
  };
};

const mapDispatchToProps: ContentionDebugDispatchProps = {
  refreshTxnContentionEvents: refreshTxnContentionEvents,
};

export default connect(
  mapStateToProps,
  mapDispatchToProps,
)(ContentionDebugPage);
