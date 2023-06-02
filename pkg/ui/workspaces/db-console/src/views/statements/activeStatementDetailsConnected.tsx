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
  ActiveStatementDetails,
  ActiveStatementDetailsStateProps,
  ActiveStatementDetailsDispatchProps,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { AdminUIState } from "src/redux/state";
import { refreshLiveWorkload } from "src/redux/apiReducers";
import {
  selectActiveStatement,
  selectContentionDetailsForStatement,
} from "src/selectors";
import { selectHasAdminRole } from "src/redux/user";

export default withRouter(
  connect<
    ActiveStatementDetailsStateProps,
    ActiveStatementDetailsDispatchProps,
    RouteComponentProps
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
