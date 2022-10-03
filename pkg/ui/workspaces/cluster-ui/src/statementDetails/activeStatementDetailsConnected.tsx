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
import { ActiveStatementDetailsStateProps } from ".";
import {
  selectActiveStatement,
  selectContentionDetailsForStatement,
} from "src/selectors/activeExecutions.selectors";
import { selectIsTenant } from "src/store/uiConfig";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): ActiveStatementDetailsStateProps => {
  return {
    contentionDetails: selectContentionDetailsForStatement(state, props),
    statement: selectActiveStatement(state, props),
    match: props.match,
    isTenant: selectIsTenant(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): ActiveStatementDetailsDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
});

export const ActiveStatementDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ActiveStatementDetails),
);
