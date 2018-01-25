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
import { refreshNodes, refreshLiveness, refreshLocations } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { selectLocationsRequestStatus, selectLocationTree, Location, LocationTree } from "src/redux/locations";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { findMostSpecificLocation } from "src/util/locations";

import { ZoomTransformer } from "./zoom";
import { ModalLocalitiesView } from "./modalLocalities";

type NodeStatus = protos.cockroach.server.status.NodeStatus$Properties;

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
  private location: Location;

  constructor(initialStatus: NodeStatus, location: Location) {
    this.statusHistory = [initialStatus];
    this.computeClientActivityRate();
    this.location = location;
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
    if (_.isNil(this.location)) {
      throw new Error("Node does not have location assigned!");
    }

    return ([this.location.longitude, this.location.latitude] as [number, number]);
  }

  tiers() {
    return this.statusHistory[0].desc.locality.tiers;
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
  locationTree: LocationTree;
  locationStatus: CachedDataReducerState<any>;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshLocations: typeof refreshLocations;
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
    if (!props.nodesSummary.nodeStatuses || !props.locationStatus.valid) {
      return;
    }

    props.nodesSummary.nodeStatuses.map((status) => {
      const id = status.desc.node_id;
      if (!this.nodeHistories.hasOwnProperty(id)) {
        // Yet another problem caused by the Protobuf.js-generated types.
        const tiers = status.desc.locality.tiers.map(({ key, value }) => ({ key, value }));

        const loc = findMostSpecificLocation(props.locationTree, tiers);
        this.nodeHistories[id] = new SimulatedNodeStatus(status, loc);
      } else {
        this.nodeHistories[id].update(status);
      }
    });
  }

  componentWillMount() {
    this.accumulateHistory();
    this.props.refreshNodes();
    this.props.refreshLiveness();
    this.props.refreshLocations();
  }

  componentWillReceiveProps(props: NodeSimulatorProps & NodeSimulatorOwnProps) {
    this.accumulateHistory(props);
    props.refreshNodes();
    props.refreshLiveness();
    props.refreshLocations();
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
    locationTree: selectLocationTree(state),
    locationStatus: selectLocationsRequestStatus(state),
  }),
  {
    refreshNodes,
    refreshLiveness,
    refreshLocations,
  },
)(NodeSimulator);
