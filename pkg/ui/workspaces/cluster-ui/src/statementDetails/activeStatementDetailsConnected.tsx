// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps, withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { actions as sessionsActions } from "src/store/sessions";
import { AppState } from "../store";
import {
  ActiveStatementDetails,
  ActiveStatementDetailsDispatchProps,
} from "./activeStatementDetails";
import { selectActiveStatement } from "./activeStatementDetails.selectors";
import { ActiveStatementDetailsStateProps } from ".";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): ActiveStatementDetailsStateProps => {
  return {
    statement: selectActiveStatement(state, props),
    match: props.match,
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): ActiveStatementDetailsDispatchProps => ({
  refreshSessions: () => dispatch(sessionsActions.refresh()),
});

export const ActiveStatementDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ActiveStatementDetails),
);
