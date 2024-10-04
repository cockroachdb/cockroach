// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  RecentStatementDetails,
  RecentStatementDetailsStateProps,
  RecentStatementDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import { refreshLiveWorkload } from "src/redux/apiReducers";
import {
  selectRecentStatement,
  selectContentionDetailsForStatement,
} from "src/selectors";
import { selectHasAdminRole } from "src/redux/user";

export default withRouter(
  connect<
    RecentStatementDetailsStateProps,
    RecentStatementDetailsDispatchProps,
    RouteComponentProps
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      match: props.match,
      statement: selectRecentStatement(state, props),
      contentionDetails: selectContentionDetailsForStatement(state, props),
      hasAdminRole: selectHasAdminRole(state),
    }),
    { refreshLiveWorkload },
  )(RecentStatementDetails),
);
