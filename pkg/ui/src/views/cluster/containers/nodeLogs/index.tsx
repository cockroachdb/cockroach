// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import React from "react";
import { Helmet } from "react-helmet";
import { RouterState } from "react-router";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { INodeStatus } from "src/util/proto";
import { nodeIDAttr, REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import { LogEntriesResponseMessage } from "src/util/api";
import { LongToMoment } from "src/util/convert";
import { SortableTable } from "src/views/shared/components/sortabletable";
import { AdminUIState } from "src/redux/state";
import { refreshLogs, refreshNodes } from "src/redux/apiReducers";
import { currentNode } from "src/views/cluster/containers/nodeOverview";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { getDisplayName } from "src/redux/nodes";
import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import "./logs.styl";

interface LogProps {
  logs: CachedDataReducerState<LogEntriesResponseMessage>;
  currentNode: INodeStatus;
  refreshLogs: typeof refreshLogs;
  refreshNodes: typeof refreshNodes;
}

/**
 * Renders the main content of the logs page.
 */
class Logs extends React.Component<LogProps & RouterState, {}> {
  componentWillMount() {
    this.props.refreshNodes();
    this.props.refreshLogs(new protos.cockroach.server.serverpb.LogsRequest({ node_id: this.props.params[nodeIDAttr] }));
  }

  render() {
    const nodeAddress = this.props.currentNode
      ? this.props.currentNode.desc.address.address_field
      : null;

    const title = this.props.currentNode
      ? `Logs | ${getDisplayName(this.props.currentNode)} | Nodes`
      : `Logs | Node ${this.props.params[nodeIDAttr]} | Nodes`;

    // TODO(couchand): This is a really myopic way to check for this particular
    // case, but making major changes to the CachedDataReducer or util.api seems
    // fraught at this point.  We should revisit this soon.
    if (this.props.logs.lastError && this.props.logs.lastError.message === "Forbidden") {
      return (
        <div>
          <Helmet>
            <title>{ title }</title>
          </Helmet>
          <div className="section section--heading">
            <h2>Logs Node { this.props.params[nodeIDAttr] } / { nodeAddress }</h2>
          </div>
          <section className="section">
            { REMOTE_DEBUGGING_ERROR_TEXT }
          </section>
        </div>
      );
    }

    let content: React.ReactNode = "No data";

    if (this.props.logs.data) {
      const logEntries = _.sortBy(this.props.logs.data.entries, (e) => e.time);
      const columns = [
        {
          title: "Time",
          cell: (index: number) => LongToMoment(logEntries[index].time).format("YYYY-MM-DD HH:mm:ss"),
        },
        {
          title: "Severity",
          cell: (index: number) => protos.cockroach.util.log.Severity[logEntries[index].severity],
        },
        {
          title: "Message",
          cell: (index: number) => (
            <pre className="sort-table__unbounded-column logs-table__message">
              { logEntries[index].message }
            </pre>
          ),
        },
        {
          title: "File:Line",
          cell: (index: number) => `${logEntries[index].file}:${logEntries[index].line}`,
        },
      ];
      content = (
        <SortableTable
          count={logEntries.length}
          columns={columns}
          className="logs-table"
        />
      );
    }
    return (
      <div>
        <Helmet>
          <title>{ title }</title>
        </Helmet>
        <div className="section section--heading">
          <h2>Logs Node { this.props.params[nodeIDAttr] } / { nodeAddress }</h2>
        </div>
        <section className="section">
          <Loading
            loading={ !this.props.logs.data }
            className="loading-image loading-image__spinner-left"
            image={ spinner }
            render={() => content}
          />
        </section>
      </div>
    );
  }
}

// Connect the EventsList class with our redux store.
const logsConnected = connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      logs: state.cachedData.logs,
      currentNode: currentNode(state, ownProps),
    };
  },
  {
    refreshLogs,
    refreshNodes,
  },
)(Logs);

export default logsConnected;
