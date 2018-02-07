// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { connect } from "react-redux";

import { refreshNodes, refreshLiveness, refreshLocations } from "src/redux/apiReducers";
import { selectLocalityTree, LocalityTier, LocalityTree } from "src/redux/localities";
import { selectLocationsRequestStatus, selectLocationTree, LocationTree } from "src/redux/locations";
import {
  nodesSummarySelector,
  NodesSummary,
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  livenessStatusByNodeIDSelector,
  LivenessStatus,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import Loading from "src/views/shared/components/loading";

import { NodeCanvas } from "./nodeCanvas";
import { NodeHistory } from "./nodeHistory";

import spinner from "assets/spinner.gif";

interface NodeSimulatorProps {
  nodesSummary: NodesSummary;
  localityTree: LocalityTree;
  locationTree: LocationTree;
  liveness: { [id: string]: LivenessStatus };
  dataIsValid: boolean;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
  refreshLocations: typeof refreshLocations;
}

interface NodeSimulatorOwnProps {
  tiers: LocalityTier[];
}

class NodeSimulator extends React.Component<NodeSimulatorProps & NodeSimulatorOwnProps, any> {
  nodeHistories: { [id: string]: NodeHistory } = {};

  // accumulateHistory parses incoming nodeStatus properties and accumulates
  // a history for each node.
  accumulateHistory(props = this.props) {
    if (!props.dataIsValid) {
      return;
    }

    props.nodesSummary.nodeStatuses.map((status) => {
      const id = status.desc.node_id;
      if (!this.nodeHistories.hasOwnProperty(id)) {
        this.nodeHistories[id] = new NodeHistory(status);
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
      <Loading
        loading={!this.props.dataIsValid}
        className="loading-image loading-image__spinner-left"
        image={spinner}
      >
        <NodeCanvas
          nodeHistories={this.nodeHistories}
          localityTree={this.props.localityTree}
          locationTree={this.props.locationTree}
          liveness={this.props.liveness}
          tiers={this.props.tiers}
        />
      </Loading>
    );
  }
}

export default connect(
  (state: AdminUIState, _ownProps: NodeSimulatorOwnProps) => ({
    nodesSummary: nodesSummarySelector(state),
    localityTree: selectLocalityTree(state),
    locationTree: selectLocationTree(state),
    liveness: livenessStatusByNodeIDSelector(state),
    dataIsValid:
      selectNodeRequestStatus(state).valid
      && selectLocationsRequestStatus(state).valid
      && selectLivenessRequestStatus(state).valid,
  }),
  {
    refreshNodes,
    refreshLiveness,
    refreshLocations,
  },
)(NodeSimulator);
