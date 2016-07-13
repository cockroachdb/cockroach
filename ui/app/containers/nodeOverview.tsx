import * as React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import { IInjectedProps } from "react-router";
import _ = require("lodash");

import { nodeID } from "./../util/constants";
import { AdminUIState } from "../redux/state";
import { refreshNodes } from "../redux/apiReducers";
import { NodeStatus, MetricConstants } from  "../util/proto";
import { Bytes, Percentage } from "../util/format";

interface NodeOverviewProps extends IInjectedProps {
  node: NodeStatus;
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
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.liveBytes))} />
              <TableRow data={node}
                        title="Key Bytes"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.keyBytes))} />
              <TableRow data={node}
                        title="Value Bytes"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.valBytes))} />
              <TableRow data={node}
                        title="Intent Bytes"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.intentBytes))} />
              <TableRow data={node}
                        title="Sys Bytes"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.sysBytes))} />
              <TableRow data={node}
                        title="GC Bytes Age"
                        valueFn={(metrics) => metrics.get(MetricConstants.gcBytesAge).toString()} />
              <TableRow data={node}
                        title="Total Replicas"
                        valueFn={(metrics) => metrics.get(MetricConstants.replicas).toString()} />
              <TableRow data={node}
                        title="Leader Ranges"
                        valueFn={(metrics) => metrics.get(MetricConstants.leaderRanges).toString()} />
              <TableRow data={node}
                        title="Available"
                        valueFn={(metrics) => Percentage(metrics.get(MetricConstants.availableRanges), metrics.get(MetricConstants.leaderRanges))} />
              <TableRow data={node}
                        title="Fully Replicated"
                        valueFn={(metrics) => Percentage(metrics.get(MetricConstants.replicatedRanges), metrics.get(MetricConstants.leaderRanges))} />
              <TableRow data={node}
                        title="Available Capacity"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.availableCapacity))} />
              <TableRow data={node}
                        title="Total Capacity"
                        valueFn={(metrics) => Bytes(metrics.get(MetricConstants.capacity))} />
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
function TableRow(props: { data: NodeStatus, title: string, valueFn: (s: cockroach.ProtoBufMap<string, number>) => React.ReactNode }) {
  return <tr>
    <td className="title">{ props.title }</td>
    <td className="value">{ props.valueFn(props.data.metrics) }</td>
    {
      _.map(props.data.store_statuses,
            (ss) => <td key={ss.desc.store_id} className="value">{ props.valueFn(ss.metrics) }</td>
           )
    }
  </tr>;
}

let currentNode = createSelector(
  (state: AdminUIState, props: IInjectedProps): NodeStatus[] => state.cachedData.nodes.data,
  (state: AdminUIState, props: IInjectedProps): number => parseInt(props.params[nodeID], 10),
  (nodes, id) => {
    if (!nodes || !id) {
      return undefined;
    }
    return _.find(nodes, (ns) => ns.desc.node_id === id);
  });

export default connect(
  (state: AdminUIState, ownProps: IInjectedProps) => {
    return {
      node: currentNode(state, ownProps),
    };
  },
  {
    refreshNodes,
  }
)(NodeOverview);
