// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { Dispatch } from "redux";

import {
  selecteActiveStatement,
  selectContentionDetailsForStatement,
} from "src/selectors/activeExecutions.selectors";
import { actions as sessionsActions } from "src/store/sessions";
import { selectHasAdminRole } from "src/store/uiConfig";

import { AppState } from "../store";

import {
  ActiveStatementDetails,
  ActiveStatementDetailsDispatchProps,
} from "./activeStatementDetails";

import { ActiveStatementDetailsStateProps } from ".";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): ActiveStatementDetailsStateProps => {
  return {
    contentionDetails: selectContentionDetailsForStatement(state, props),
    statement: selecteActiveStatement(state, props),
    match: props.match,
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): ActiveStatementDetailsDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
});

export const RecentStatementDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ActiveStatementDetails),
);

// Prior to 23.1, this component was called
// ActiveStatementDetailsPageConnected. We keep the alias here to avoid
// breaking the multi-version support in managed-service's console code.
// When managed-service drops support for 22.2 (around the end of 2024?),
// we can remove this code.
export const ActiveStatementDetailsPageConnected =
  RecentStatementDetailsPageConnected;
