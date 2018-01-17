// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import * as d3 from "d3";
import * as protos from "src/js/protos";

import { NanoToMilli } from "src/util/convert";
import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";

import { ZoomTransformer } from "./zoom";
import { ModalLocalitiesView } from "./modalLocalities";

type NodeStatus = protos.cockroach.server.status.NodeStatus$Properties;
type Tier = protos.cockroach.roachpb.Tier$Properties;

// List of fake location data in order to place nodes on the map for visual
// effect.
const locations: [number, number][] = [
  [-74.00597, 40.71427],
  [-80.19366, 25.77427],
  [-93.60911, 41.60054],
  [-118.24368, 34.05223],
  [-122.33207, 47.60621],
  [-0.12574, 51.50853],
  [13.41053, 52.52437],
  [18.0649, 59.33258],
  [151.20732, -33.86785],
  [144.96332, -37.814],
  [153.02809, -27.46794],
  [116.39723, 39.9075],
  [121.45806, 31.22222],
  [114.0683, 22.54554],
  [72.88261, 19.07283],
  [77.59369, 12.97194],
  [77.22445, 28.63576],
];

const localities: Tier[][] = [
  [
    { key: "datacenter", value: "us-east" },
    { key: "rack", value: "us-east-1" },
  ],
  [
    { key: "datacenter", value: "us-east" },
    { key: "rack", value: "us-east-2" },
  ],
  [
    { key: "datacenter", value: "us-east" },
    { key: "rack", value: "us-east-3" },
  ],
  [
    { key: "datacenter", value: "us-west" },
    { key: "rack", value: "us-west-1" },
  ],
  [
    { key: "datacenter", value: "us-west" },
    { key: "rack", value: "us-west-2" },
  ],
];

// SimulatedNodeStatus augments a node status from the redux state with
// additional simulated information in order to facilitate testing of the
// cluster visualization.
//
// TODO(mrtracy): This layer is a temporary measure needed for testing during
// initial development. The simulator should be removed before official release,
// with the simulated data coming from real sources.
//
// + HACK: Maintains old versions of each node status history, allowing for the
//   computation of instantaneous rates without querying time series. This
//   functionality should be moved to the reducer.
// + Simulates long/lat coordinates for each node. This will eventually be
//   provided by the backend via a system table.
// + Simulates locality tiers for each node. These are already available from
//   the nodes, but the simulated localities allow testing more complicated
//   layouts.
export class SimulatedNodeStatus {
  // "Client Activity" is a generic measurement present on each locality in
  // the current prototype design.
  // Currently, it is the to number SQL operations executed per second,
  // computed from the previous two node statuses.
  clientActivityRate: number;
  private statusHistory: NodeStatus[];
  private maxHistory = 2;

  constructor(initialStatus: NodeStatus) {
    this.statusHistory = [initialStatus];
    this.computeClientActivityRate();
  }

  update(nextStatus: NodeStatus) {
    if (this.statusHistory[0].updated_at.lessThan(nextStatus.updated_at)) {
      this.statusHistory.unshift(nextStatus);
      if (this.statusHistory.length > this.maxHistory) {
        this.statusHistory.pop();
      }

      this.computeClientActivityRate();
    }
  }

  id() {
    return this.statusHistory[0].desc.node_id;
  }

  latest() {
    return this.statusHistory[0];
  }

  longLat() {
    return locations[this.id() % locations.length];
  }

  tiers() {
    return localities[this.id() % localities.length];
  }

  private computeClientActivityRate() {
    this.clientActivityRate = 0;
    if (this.statusHistory.length > 1) {
      const [latest, prev] = this.statusHistory;
      const seconds = NanoToMilli(latest.updated_at.subtract(prev.updated_at).toNumber()) / 1000;
      const totalOps = (latest.metrics["sql.select.count"] - prev.metrics["sql.select.count"]) +
        (latest.metrics["sql.update.count"] - prev.metrics["sql.update.count"]) +
        (latest.metrics["sql.insert.count"] - prev.metrics["sql.insert.count"]) +
        (latest.metrics["sql.delete.count"] - prev.metrics["sql.delete.count"]);
      this.clientActivityRate = totalOps / seconds;
    }
  }
}

interface NodeSimulatorProps {
  nodesSummary: NodesSummary;
  statusesValid: boolean;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

interface NodeSimulatorOwnProps {
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
}

// NodeSimulator augments real node data with information that is not yet
// available, but necessary in order to display the simulation.
//
// TODO(mrtracy): This layer is a temporary measure needed for testing during
// initial development. The simulator should be removed before official release,
// with the simulated data coming from real sources.
//
// HACK: nodeHistories is used to maintain a list of previous node statuses
// for individual nodes. This is used to display rates of change without
// having to query time series data. This is a hack because this behavior
// should be moved into the reducer or into a higher order component.
class NodeSimulator extends React.Component<NodeSimulatorProps & NodeSimulatorOwnProps, any> {
  nodeHistories: { [id: string]: SimulatedNodeStatus } = {};

  // accumulateHistory parses incoming nodeStatus properties and accumulates
  // a history for each node.
  accumulateHistory(props = this.props) {
    if (!props.nodesSummary.nodeStatuses) {
      return;
    }

    props.nodesSummary.nodeStatuses.map((status) => {
      const id = status.desc.node_id;
      if (!this.nodeHistories.hasOwnProperty(id)) {
        this.nodeHistories[id] = new SimulatedNodeStatus(status);
      } else {
        this.nodeHistories[id].update(status);
      }
    });
  }

  componentWillMount() {
    this.accumulateHistory();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: NodeSimulatorProps & NodeSimulatorOwnProps) {
    this.accumulateHistory(props);
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    return (
      <ModalLocalitiesView
        nodeHistories={_.values(this.nodeHistories)}
        projection={this.props.projection}
        zoom={this.props.zoom}
      />
    );
  }
}

export default connect(
  (state: AdminUIState, _ownProps: NodeSimulatorOwnProps) => ({
    nodesSummary: nodesSummarySelector(state),
    statusesValid: state.cachedData.nodes.valid && state.cachedData.liveness.valid,
  }),
  {
    refreshNodes,
    refreshLiveness,
  },
)(NodeSimulator);
