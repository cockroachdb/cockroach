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
import {createStatementDiagnosticsRequest, StatementDiagnosticsRequestsResponseMessage} from "src/util/api";
import {cockroach} from "src/js/protos";
import CreateStatementDiagnosticsRequestRequest = cockroach.server.serverpb.CreateStatementDiagnosticsRequestRequest;

interface SDRProps {
  requests: CachedDataReducerState<StatementDiagnosticsRequestsResponseMessage>;
  refreshStatementDiagnosticsRequests: typeof refreshStatementDiagnosticsRequests;
  sendRequest: typeof createStatementDiagnosticsRequest;
}

export class StatementDiagnosticsRequests extends React.Component<SDRProps & RouteComponentProps> {
  componentDidMount() {
    this.props.refreshStatementDiagnosticsRequests();
  }

  sendRequest() {
    const message = new CreateStatementDiagnosticsRequestRequest;
    message.statement_fingerprint = "INSERT INTO test2 VALUES (_)"
    this.props.sendRequest(message).then(() => { alert("sent!"); });
  }

  render() {
    return (
      <Fragment>
        <Helmet title="Statement Diagnostics Requests" />
        <div className="content">
          {this.props.requests.data ? JSON.stringify(this.props.requests.data.toJSON()) : "Loading..."}
        </div>
        <button onClick={this.sendRequest.bind(this)}>
          Hiya
        </button>
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
  sendRequest: createStatementDiagnosticsRequest,
};

export default withRouter(connect(mapStateToProps, mapDispatchToProps)(StatementDiagnosticsRequests));
