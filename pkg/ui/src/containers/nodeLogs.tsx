import _ from "lodash";
import * as React from "react";
import { RouterState } from "react-router";
import { AdminUIState } from "../redux/state";
import { refreshLogs } from "../redux/apiReducers";
import { connect } from "react-redux";

import * as protos from "../js/protos";
import { nodeIDAttr } from "../util/constants";
import { LogEntriesResponseMessage } from "../util/api";
import { LongToMoment } from "../util/convert";

import { SortableTable } from "../components/sortabletable";

interface LogProps {
  logs: LogEntriesResponseMessage;
  refreshLogs: typeof refreshLogs;
}

/**
 * Renders the main content of the help us page.
 */
class Logs extends React.Component<LogProps & RouterState, {}> {

  componentWillMount() {
    this.props.refreshLogs(new protos.cockroach.server.serverpb.LogsRequest({ node_id: this.props.params[nodeIDAttr] }));
  }

  render() {
    if (this.props.logs) {
      let logEntries = _.sortBy(this.props.logs.entries, (e) => e.time);
      const columns = [
        {
          title: "Time",
          cell: (index: number) => LongToMoment(logEntries[index].time).format("YYYY-MM-DD HH:mm:SS"),
        },
        {
          title: "Severity",
          cell: (index: number) => protos.cockroach.util.log.Severity[logEntries[index].severity],
        },
        {
          title: "Message",
          cell: (index: number) => logEntries[index].message,
        },
        {
          title: "File:Line",
          cell: (index: number) => `${logEntries[index].file}:${logEntries[index].line}`,
        },
      ];
      return <div className="logs-table">
        <SortableTable count={logEntries.length} columns={columns}>
        </SortableTable>
      </div>;
    }
    return <div className="logs-table">
      No data.
    </div>;
  }
}

let logs = (state: AdminUIState): LogEntriesResponseMessage => state.cachedData.logs.data;

// Connect the EventsList class with our redux store.
let logsConnected = connect(
  (state: AdminUIState) => {
    return {
      logs: logs(state),
    };
  },
  {
    refreshLogs,
  },
)(Logs);

export default logsConnected;
