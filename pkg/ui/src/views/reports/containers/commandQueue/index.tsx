// import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import { RouterState } from "react-router";

// import * as protos from "src/js/protos";
import { refreshRange, refreshAllocatorRange } from "src/redux/apiReducers";
// import { AdminUIState } from "src/redux/state";
import { rangeIDAttr } from "src/util/constants";
// import ConnectionsTable from "src/views/reports/containers/range/connectionsTable";

interface CommandQueueOwnProps {
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

  render() {
    const rangeID = this.props.params[rangeIDAttr];

    return (
      <div className="section">
        <h1>Command queue report for r{rangeID.toString()}</h1>
      </div>
    );
  }
}

function mapStateToProps() {
  return {};
}

const actions = {
  refreshRange,
  refreshAllocatorRange,
};

export default connect(mapStateToProps, actions)(CommandQueue);
