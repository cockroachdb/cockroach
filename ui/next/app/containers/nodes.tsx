/// <reference path="../../typings/main.d.ts" />

import * as React from "react";
import * as _ from "lodash";
import { connect } from "react-redux";

import { refreshNodes, NodeStatusState } from "../redux/nodes";
import { NodeStatus } from "../interfaces/proto";
import { MetricConstants } from "../util/proto";

/**
 * NodesMainProps are the properties which can be passed to the NodesMain
 * container.
 */
interface NodesMainProps {
  // The currently cached nodes status query.
  state: NodeStatusState;
  // When called, attempts to refresh the nodes status query.
  refreshNodes(): void;
}

/**
 * NodesMain renders the main content of the nodes page.
 */
class NodesMain extends React.Component<NodesMainProps, {}> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: NodesMainProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
  }

  render() {
    // Display each node's CPU usage. This is simply a proof of concept that the
    // query system works.
    let rows = _.map(this.props.state.statuses, (ns: NodeStatus) => {
      return <div key={ns.desc.node_id}>{ns.metrics[MetricConstants.userCPUPercent]}</div>;
    });

    return <div className="section">
      <h1>Node Ids:</h1>
      { rows }
    </div>;
  }
}

// 'Connect' the NodesMain class with redux.
let nodesMainConnected = connect(
  (state, ownProps) => {
    return {
      state: state.nodes,
    };
  },
  {
    refreshNodes: refreshNodes,
  }
)(NodesMain);

export { nodesMainConnected as NodesMain };

/**
 * NodesTitle renders the header of the nodes page.
 */
export class NodesTitle extends React.Component<{}, {}> {
  render() {
    return <h2>Nodes</h2>;
  }
}

