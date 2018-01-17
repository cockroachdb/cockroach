import _ from "lodash";
import Long from "long";
import { Link, RouterState } from "react-router";
import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { commandQueueRequestKey, refreshCommandQueue } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import Print from "src/views/reports/containers/range/print";
import Loading from "src/views/shared/components/loading";
import CommandQueueViz from "src/views/reports/containers/commandQueue/commandQueueViz";

import spinner from "assets/spinner.gif";

interface CommandQueueOwnProps {
  commandQueue: CachedDataReducerState<protos.cockroach.server.serverpb.CommandQueueResponse>;
  refreshCommandQueue: typeof refreshCommandQueue;
}

type CommandQueueProps = CommandQueueOwnProps & RouterState;

function commandQueueRequestFromProps(props: CommandQueueProps) {
  return new protos.cockroach.server.serverpb.CommandQueueRequest({
    range_id: Long.fromString(props.params[rangeIDAttr]),
  });
}

/**
 * Renders the Command Queue Report page.
 */
class CommandQueue extends React.Component<CommandQueueProps, {}> {

  refresh(props = this.props) {
    props.refreshCommandQueue(commandQueueRequestFromProps(props));
  }

  componentWillMount() {
    this.refresh();
  }

  renderReportBody() {
    const commandQueue = this.props.commandQueue;
    if (_.isNil(commandQueue)) {
      return null;
    }
    if (commandQueue && !_.isNil(commandQueue.lastError)) {
      return (
        <div>
          <h2>Error loading the command queue:</h2>
          {commandQueue.lastError.toString()}
        </div>
      );
    }

    if (_.isNil(commandQueue.data) || _.isNil(commandQueue.data.snapshot)) {
      return (
        <div>
          <h2>Error</h2>
          "No command queue data was returned."
        </div>
      );
    }

    const snapshot = commandQueue.data.snapshot;

    return (
      <div>
        <div className="command-queue__timestamp">
          <span>
            Snapshot taken at
            {" "}{Print.Timestamp(snapshot.timestamp)}
          </span>
        </div>
        <div className="command-queue__key">
          Key:
          <div className="command-queue__key__read">Read</div>
          <div className="command-queue__key__write">Write</div>
        </div>

        <h2>Local Scope</h2>
        <CommandQueueViz queue={snapshot.localScope} />

        <h2>Global Scope</h2>
        <CommandQueueViz queue={snapshot.globalScope} />
      </div>
    );
  }

  render() {
    const rangeID = this.props.params[rangeIDAttr];
    return (
      <div className="section command-queue">
        <h1>
          <Link
            to={`/reports/range/${rangeID.toString()}`}
            className="debug-link">
            r{rangeID.toString()}
          </Link>
          {" > "}
          Command queue
        </h1>
        <Loading
          loading={!this.props.commandQueue || this.props.commandQueue.inFlight}
          className="loading-image loading-image__spinner-left"
          image={spinner}
        >
          {this.renderReportBody()}
        </Loading>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: CommandQueueProps) {
  const commandQueueKey = commandQueueRequestKey(commandQueueRequestFromProps(props));
  return {
    commandQueue: state.cachedData.commandQueue[commandQueueKey],
  };
}

const actions = {
  refreshCommandQueue,
};

export default connect(mapStateToProps, actions)(CommandQueue);
