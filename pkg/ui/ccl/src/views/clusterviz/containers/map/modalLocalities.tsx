// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import * as d3 from "d3";
import _, { Dictionary } from "lodash";
import PropTypes from "prop-types";
import React from "react";
import { InjectedRouter, RouterState } from "react-router";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLeaves, getLocality } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";
import { NodeStatus$Properties } from "src/util/proto";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { NodeView } from "./nodeView";
import { Box, ZoomTransformer } from "./zoom";
import { StatsView } from "ccl/src/views/clusterviz/containers/map/statsView";
import { sumNodeStats } from "src/redux/nodes";
import { cockroach } from "src/js/protos";

type NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;

interface LocalityViewProps {
  localityTree: LocalityTree;
  liveness: Dictionary<NodeLivenessStatus>;
}

class LocalityView extends React.Component<LocalityViewProps, any> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  onClick = () => {
    const destination =
      CLUSTERVIZ_ROOT + "/" + generateLocalityRoute(this.props.localityTree.tiers);
    this.context.router.push(destination);
  }

  render() {
    const { tiers } = this.props.localityTree;
    const thisTier = tiers[tiers.length - 1];

    const leavesUnderMe = getLeaves(this.props.localityTree);
    const { capacityUsable, capacityUsed } = sumNodeStats(leavesUnderMe, this.props.liveness);

    return (
      <g onClick={this.onClick} style={{ cursor: "pointer" }}>
        <StatsView
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
          label={`${thisTier.key}=${thisTier.value}`}
        />
      </g>
    );
  }
}

interface LocalityBoxProps {
  projection: d3.geo.Projection;
  localityTree: LocalityTree;
  locationTree: LocationTree;
  liveness: Dictionary<NodeLivenessStatus>;
}

class LocalityBox extends React.Component<LocalityBoxProps, any> {
  render() {
    const location = findOrCalculateLocation(this.props.locationTree, this.props.localityTree);
    const center = this.props.projection([location.longitude, location.latitude]);
    const box = new Box(center[0] - 50, center[1] - 50, 100, 100);
    return (
      <g transform={`translate(${box.center()})`}>
        <LocalityView localityTree={this.props.localityTree} liveness={this.props.liveness} />
      </g>
    );
  }
}

interface NodeBoxProps {
  node: NodeStatus$Properties;
  liveness: Dictionary<NodeLivenessStatus>;
}

class NodeBox extends React.Component<NodeBoxProps, any> {
  // TODO: layout!
  render() {
    return (
      <g>
        <NodeView
          node={this.props.node}
          liveness={this.props.liveness}
        />
      </g>
    );
  }
}

interface ModalLocalitiesViewProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  tiers: LocalityTier[];
  nodeHistories: { [id: string]: SimulatedNodeStatus };
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
  liveness: Dictionary<NodeLivenessStatus>;
}

export class ModalLocalitiesView extends React.Component<ModalLocalitiesViewProps, any> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  renderChildLocalities(tree: LocalityTree) {
    const children: React.ReactNode[] = [];

    _.values(tree.localities).forEach((tier) => {
      _.values(tier).forEach((locality) => {
        children.push(
          <LocalityBox
            projection={this.props.projection}
            localityTree={locality}
            locationTree={this.props.locationTree}
            liveness={this.props.liveness}
          />,
        );
      });
    });

    return children;
  }

  renderChildNodes(tree: LocalityTree) {
    const children: React.ReactNode[] = [];

    tree.nodes.forEach((node) => {
      children.push(
        <NodeBox node={node} liveness={this.props.liveness} />,
      );
    });

    return children;
  }

  render() {
    const treeToRender = getLocality(this.props.localityTree, this.props.tiers);
    if (_.isNil(treeToRender)) {
      this.context.router.replace(CLUSTERVIZ_ROOT);
    }

    return (
      <g>
        { this.renderChildLocalities(treeToRender) }
        { this.renderChildNodes(treeToRender) }
      </g>
    );
  }
}
