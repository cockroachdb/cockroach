// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  SortedTable,
  util,
  Timestamp,
} from "@cockroachlabs/cluster-ui";
import sortBy from "lodash/sortBy";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";

import * as protos from "src/js/protos";
import { refreshLogs, refreshNodes } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { getDisplayName } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { LogEntriesResponseMessage } from "src/util/api";
import { nodeIDAttr } from "src/util/constants";
import { INodeStatus } from "src/util/proto";
import { getMatchParamByName } from "src/util/query";
import { currentNode } from "src/views/cluster/containers/nodeOverview";
import "./logs.styl";

type LogEntries = protos.cockroach.util.log.IEntry;

interface LogProps {
  logs: CachedDataReducerState<LogEntriesResponseMessage>;
  currentNode: INodeStatus;
  refreshLogs: typeof refreshLogs;
  refreshNodes: typeof refreshNodes;
}

/**
 * Renders the main content of the logs page.
 */
export class Logs extends React.Component<LogProps & RouteComponentProps, {}> {
  componentDidMount() {
    const nodeId = getMatchParamByName(this.props.match, nodeIDAttr);
    this.props.refreshNodes();
    this.props.refreshLogs(
      new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
    );
  }

  renderContent = () => {
    const logEntries = sortBy(this.props.logs.data.entries, e => e.time);
    const columns = [
      {
        title: "Time",
        name: "time",
        cell: (logEntry: LogEntries) => (
          <Timestamp
            time={util.LongToMoment(logEntry.time)}
            format={util.DATE_WITH_SECONDS_FORMAT_24_TZ}
          />
        ),
      },
      {
        title: "Severity",
        name: "severity",
        cell: (logEntry: LogEntries) =>
          protos.cockroach.util.log.Severity[logEntry.severity],
      },
      {
        title: "Message",
        name: "message",
        cell: (logEntry: LogEntries) => (
          <pre className="sort-table__unbounded-column logs-table__message">
            {(logEntry.tags ? "[" + logEntry.tags + "] " : "") +
              logEntry.message}
          </pre>
        ),
      },
      {
        title: "File:Line",
        name: "file",
        cell: (logEntry: LogEntries) => `${logEntry.file}:${logEntry.line}`,
      },
    ];
    return (
      <SortedTable data={logEntries} columns={columns} className="logs-table" />
    );
  };

  render() {
    const nodeAddress = this.props.currentNode
      ? this.props.currentNode.desc.address.address_field
      : null;
    const nodeId = getMatchParamByName(this.props.match, nodeIDAttr);
    const title = this.props.currentNode
      ? `Logs | ${getDisplayName(this.props.currentNode)} | Nodes`
      : `Logs | Node ${nodeId} | Nodes`;

    return (
      <div>
        <Helmet title={title} />
        <div className="section section--heading">
          <h2 className="base-heading">
            Logs Node {nodeId} / {nodeAddress}
          </h2>
        </div>
        <section className="section">
          <Loading
            loading={!this.props.logs.data}
            page={"node logs"}
            error={this.props.logs.lastError}
            render={this.renderContent}
          />
        </section>
      </div>
    );
  }
}

// Connect the EventsList class with our redux store.
const logsConnected = withRouter(
  connect(
    (state: AdminUIState, ownProps: RouteComponentProps) => {
      return {
        logs: state.cachedData.logs,
        currentNode: currentNode(state, ownProps),
      };
    },
    {
      refreshLogs,
      refreshNodes,
    },
  )(Logs),
);

export default logsConnected;
