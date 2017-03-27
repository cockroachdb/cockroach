import * as React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { RouterState } from "react-router";
import _ from "lodash";

import { nodeIDAttr } from "./../util/constants";
import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { NodeStatus$Properties, MetricConstants, StatusMetrics } from  "../util/proto";
import { Bytes, Percentage } from "../util/format";

interface NodeOverviewProps extends RouterState {
  node: NodeStatus$Properties;
  refreshNodes: typeof refreshNodes;
}

/**
 * Renders the Node Overview page.
 */
class NodeOverview extends React.Component<NodeOverviewProps, {}> {
  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.props.refreshNodes();
  }

  componentWillReceiveProps(props: NodeOverviewProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    props.refreshNodes();
  }

  render() {
    let node = this.props.node;
    if (!node) {
      return <div className="section">
        <h1>Loading cluster status...</h1>
      </div>;
    }

    return <div className="section table">
        <div className="stats-table">
          <table>
            <thead>
              <tr>
                <th className="title" />
                <th className="value">{`Node ${node.desc.node_id}`}</th>
                {
                  _.map(node.store_statuses, (ss) => {
                    let storeId = ss.desc.store_id;
                    return <th key={storeId} className="value">{`Store ${storeId}`}</th>;
                  })
                }
              </tr>
            </thead>
            <tbody>
              <TableRow data={node}
                        title="Live Bytes"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.liveBytes])} />
              <TableRow data={node}
                        title="Key Bytes"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.keyBytes])} />
              <TableRow data={node}
                        title="Value Bytes"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.valBytes])} />
              <TableRow data={node}
                        title="Intent Bytes"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.intentBytes])} />
              <TableRow data={node}
                        title="Sys Bytes"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.sysBytes])} />
              <TableRow data={node}
                        title="GC Bytes Age"
                        valueFn={(metrics) => metrics[MetricConstants.gcBytesAge].toString()} />
              <TableRow data={node}
                        title="Total Replicas"
                        valueFn={(metrics) => metrics[MetricConstants.replicas].toString()} />
              <TableRow data={node}
                        title="Raft Leaders"
                        valueFn={(metrics) => metrics[MetricConstants.raftLeaders].toString()} />
              <TableRow data={node}
                        title="Total Ranges"
                        valueFn={(metrics) => metrics[MetricConstants.ranges]} />
              <TableRow data={node}
                        title="Unavailable"
                        valueFn={(metrics) => Percentage(metrics[MetricConstants.unavailableRanges], metrics[MetricConstants.ranges])} />
              <TableRow data={node}
                        title="Under Replicated"
                        valueFn={(metrics) => Percentage(metrics[MetricConstants.underReplicatedRanges], metrics[MetricConstants.ranges])} />
              <TableRow data={node}
                        title="Available Capacity"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.availableCapacity])} />
              <TableRow data={node}
                        title="Total Capacity"
                        valueFn={(metrics) => Bytes(metrics[MetricConstants.capacity])} />
            </tbody>
          </table>
        </div>
        <div className="section info">
          <div className="info">{ `Build: ${node.build_info.tag}` }</div>
        </div>
      </div>;
  }
}

/**
 * TableRow is a small stateless component that renders a single row in the node
 * overview table. Each row renders a store metrics value, comparing the value
 * across the different stores on the node (along with a total value for the
 * node itself).
 */
function TableRow(props: { data: NodeStatus$Properties, title: string, valueFn: (s: StatusMetrics) => React.ReactNode }) {
  return <tr>
    <td className="title">{ props.title }</td>
    <td className="value">{ props.valueFn(props.data.metrics) }</td>
    {
      _.map(props.data.store_statuses, (ss) => {
        return <td key={ss.desc.store_id} className="value">{ props.valueFn(ss.metrics) }</td>;
      })
    }
  </tr>;
}

let currentNode = createSelector(
  (state: AdminUIState, _props: RouterState): NodeStatus$Properties[] => state.cachedData.nodes.data,
  (_state: AdminUIState, props: RouterState): number => parseInt(props.params[nodeIDAttr], 10),
  (nodes, id) => {
    if (!nodes || !id) {
      return undefined;
    }
    return _.find(nodes, (ns) => ns.desc.node_id === id);
  });

export default connect(
  (state: AdminUIState, ownProps: RouterState) => {
    return {
      node: currentNode(state, ownProps),
    };
  },
  {
    refreshNodes,
  },
)(NodeOverview);
