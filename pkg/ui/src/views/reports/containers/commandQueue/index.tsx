import _ from "lodash";
import Long from "long";
import { Link, RouterState } from "react-router";
import React from "react";
import { connect } from "react-redux";

import * as protos from "src/js/protos";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { refreshCommandQueue } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
import Print from "src/views/reports/containers/range/print";
import CommandQueueViz from "src/views/reports/containers/commandQueue/commandQueueViz";

interface CommandQueueOwnProps {
  commandQueueReducerState:
    CachedDataReducerState<protos.cockroach.server.serverpb.CommandQueueResponse>;
  refreshCommandQueue: typeof refreshCommandQueue;
}

type CommandQueueProps = CommandQueueOwnProps & RouterState;

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

  renderReportBody() {
    if (_.isNil(this.props.commandQueueReducerState.data)) {
      return (<p>Loading...</p>);
    }

    const snapshot = this.props.commandQueueReducerState.data.snapshot;

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
        {this.renderReportBody()}
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState) {
  return {
    commandQueueReducerState: state.cachedData.commandQueue,
  };
}

const actions = {
  refreshCommandQueue,
};

export default connect(mapStateToProps, actions)(CommandQueue);
