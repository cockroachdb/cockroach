import Long from "long";
import React from "react";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { refreshCommandQueue } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import CommandQueueViz from "src/views/reports/containers/commandQueue/commandQueueViz";

interface CommandQueueOwnProps {
  commandQueue: protos.cockroach.server.serverpb.CommandQueueResponse;
  refreshCommandQueue: typeof refreshCommandQueue;
}

type CommandQueueProps = CommandQueueOwnProps & RouterState;

// function ErrorPage(props: {
//   rangeID: string,
//   errorText: string,
//   rangeResponse?: protos.cockroach.server.serverpb.RangeResponse;
// }) {
//   return (
//     <div className="section">
//       <h1>Range Report for r{props.rangeID}</h1>
//       <h2>{props.errorText}</h2>
//       {
//         _.isNil(props.rangeResponse) ? null : <ConnectionsTable rangeResponse={props.rangeResponse} />
//       }
//     </div>
//   );
// }

/**
 * Renders the Command Queue Report page.
 */
class CommandQueue extends React.Component<CommandQueueProps, {}> {

  refresh(props = this.props) {
    props.refreshCommandQueue(new protos.cockroach.server.serverpb.CommandQueueRequest({
      range_id: Long.fromString(props.params[rangeIDAttr]),
    }));
  }

  componentWillMount() {
    this.refresh();
  }

  render() {
    const rangeID = this.props.params[rangeIDAttr];

    return (
      <div className="section command-queue">
        <h1>Command queue report for r{rangeID.toString()}</h1>
        <div className="command-queue__key">
          Key:
          <div
            className="command-queue__key__read"
            style={{marginLeft: 5, marginRight: 5, backgroundColor: "lightgreen"}}>
            Read
          </div>
          <div
            className="command-queue__key__write"
            style={{backgroundColor: "pink"}}>
            Write
          </div>
        </div>
        <h2>Local Scope</h2>
        {this.props.commandQueue
          ? <CommandQueueViz queue={this.props.commandQueue.queues.localScope} />
          : <p>Loading...</p>}
        <h2>Global Scope</h2>
        {this.props.commandQueue
          ? <CommandQueueViz queue={this.props.commandQueue.queues.globalScope} />
          : <p>Loading...</p>}
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    commandQueue: state.cachedData.commandQueue.data,
  };
}

const actions = {
  refreshCommandQueue,
};

export default connect(mapStateToProps, actions)(CommandQueue);
