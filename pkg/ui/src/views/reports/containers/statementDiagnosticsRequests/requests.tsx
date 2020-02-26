// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { Fragment } from "react";
import Helmet from "react-helmet";
import {RouteComponentProps, withRouter} from "react-router-dom";
import { connect } from "react-redux";

import {CachedDataReducerState, refreshStatementDiagnosticsRequests} from "src/redux/apiReducers";
import {AdminUIState} from "src/redux/state";
import { Pick } from "src/util/pick";
import {StatementDiagnosticsRequestsResponseMessage} from "src/util/api";

interface SDRProps {
  requests: CachedDataReducerState<StatementDiagnosticsRequestsResponseMessage>;
  refreshStatementDiagnosticsRequests: typeof refreshStatementDiagnosticsRequests;
}

export class StatementDiagnosticsRequests extends React.Component<SDRProps & RouteComponentProps> {
  componentDidMount() {
    this.props.refreshStatementDiagnosticsRequests();
  }

  render() {
    return (
      <Fragment>
        <Helmet title="Statement Diagnostics Requests" />
        <div className="content">
          {this.props.requests.data ? this.props.requests.data.toJSON() : "Loading..."}
        </div>
      </Fragment>
    );
  }
}

type StatementDiagnosticsRequestsState = Pick<AdminUIState, "cachedData", "statementDiagnosticsRequests">;

const selectStatementDiagnosticsRequests = (state: StatementDiagnosticsRequestsState) => {
  return state.cachedData.statementDiagnosticsRequests;
};

const mapStateToProps = (state: StatementDiagnosticsRequestsState) => ({
  requests: selectStatementDiagnosticsRequests(state),
});

const mapDispatchToProps = {
  refreshStatementDiagnosticsRequests,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(StatementDiagnosticsRequests));
