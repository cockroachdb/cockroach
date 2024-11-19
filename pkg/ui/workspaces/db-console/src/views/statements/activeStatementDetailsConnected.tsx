// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  ActiveStatementDetails,
  ActiveStatementDetailsStateProps,
  ActiveStatementDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { refreshLiveWorkload } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { selectHasAdminRole } from "src/redux/user";
import {
  selectActiveStatement,
  selectContentionDetailsForStatement,
} from "src/selectors";

export default withRouter(
  connect<
    ActiveStatementDetailsStateProps,
    ActiveStatementDetailsDispatchProps,
    RouteComponentProps,
    AdminUIState
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      match: props.match,
      statement: selectActiveStatement(state, props),
      contentionDetails: selectContentionDetailsForStatement(state, props),
      hasAdminRole: selectHasAdminRole(state),
    }),
    { refreshLiveWorkload },
  )(ActiveStatementDetails),
);
