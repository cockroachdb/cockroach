import _ from "lodash";
import * as React from "react";
import { RouterState, Link } from "react-router";
import { AdminUIState } from "../redux/state";
import { refreshLogs, refreshNodes } from "../redux/apiReducers";
import { connect } from "react-redux";

import * as protos from "../js/protos";
import { NodeStatus$Properties } from "../util/proto";
import { nodeIDAttr } from "../util/constants";
import { LogEntriesResponseMessage } from "../util/api";
import { LongToMoment } from "../util/convert";
import { SortableTable } from "../components/sortabletable";

import { currentNode } from "./nodeOverview";

interface LogProps {
  logs: LogEntriesResponseMessage;
  currentNode: NodeStatus$Properties;
  refreshLogs: typeof refreshLogs;
  refreshNodes: typeof refreshNodes;
}

/**
 * Renders the main content of the help us page.
 */
class Logs extends React.Component<LogProps & RouterState, {}> {
  componentWillMount() {
    this.props.refreshNodes();
    this.props.refreshLogs(new protos.cockroach.server.serverpb.LogsRequest({ node_id: this.props.params[nodeIDAttr] }));
  }

  render() {
    let content: React.ReactNode = "No data";
    const nodeAddress = this.props.currentNode
      ? this.props.currentNode.desc.address.address_field
      : null;

    if (this.props.logs) {
      const logEntries = _.sortBy(this.props.logs.entries, (e) => e.time);
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
            <div className="sort-table__unbounded-column">
              { logEntries[index].message }
            </div>
          ),
        },
        {
          title: "File:Line",
          cell: (index: number) => `${logEntries[index].file}:${logEntries[index].line}`,
        },
      ];
      content = <SortableTable count={logEntries.length} columns={columns} />;
    }
    return (
      <div>
        <section className="section parent-link">
          <Link to="/cluster/nodes">&lt; Back to Node List</Link>
        </section>
        <div className="header header--subsection">
          Logs Node { this.props.params[nodeIDAttr] } / { nodeAddress }
        </div>
        <section className="section">
          { content }
        </section>
      </div>
    );
  }
}

let logs = (state: AdminUIState): LogEntriesResponseMessage => state.cachedData.logs.data;

// Connect the EventsList class with our redux store.
let logsConnected = connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      logs: logs(state),
      currentNode: currentNode(state, ownProps),
    };
  },
  {
    refreshLogs,
    refreshNodes,
  },
)(Logs);

export default logsConnected;
