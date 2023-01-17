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
  RecentStatementDetails,
  RecentStatementDetailsDispatchProps,
} from "./recentStatementDetails";
import { RecentStatementDetailsStateProps } from ".";
import {
  selecteRecentStatement,
  selectContentionDetailsForStatement,
} from "src/selectors/recentExecutions.selectors";
import { selectHasAdminRole, selectIsTenant } from "src/store/uiConfig";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): RecentStatementDetailsStateProps => {
  return {
    contentionDetails: selectContentionDetailsForStatement(state, props),
    statement: selecteRecentStatement(state, props),
    match: props.match,
    isTenant: selectIsTenant(state),
    hasAdminRole: selectHasAdminRole(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): RecentStatementDetailsDispatchProps => ({
  refreshLiveWorkload: () => dispatch(sessionsActions.refresh()),
});

export const RecentStatementDetailsPageConnected = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(RecentStatementDetails),
);

// Prior to 23.1, this component was called
// ActiveStatementDetailsPageConnected. We keep the alias here to avoid
// breaking the multi-version support in managed-service's console code.
// When managed-service drops support for 22.2 (around the end of 2024?),
// we can remove this code.
export const ActiveStatementDetailsPageConnected =
  RecentStatementDetailsPageConnected;
