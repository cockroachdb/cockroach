// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import * as d3 from "d3";
import _ from "lodash";
import PropTypes from "prop-types";
import React from "react";
import { InjectedRouter, RouterState } from "react-router";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLocality } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";
import { NodeStatus$Properties } from "src/util/proto";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { NodeView } from "./nodeView";
import { Box, ZoomTransformer } from "./zoom";

interface LocalityViewProps {
  locality: LocalityTree;
}

class LocalityView extends React.Component<LocalityViewProps, any> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  onClick = () => {
    const destination = CLUSTERVIZ_ROOT + "/" + generateLocalityRoute(this.props.locality.tiers);
    this.context.router.push(destination);
  }

  render() {
    const { tiers } = this.props.locality;
    const thisTier = tiers[tiers.length - 1];

    return (
      <text x={15} y={15} onClick={this.onClick} style={{ cursor: "pointer" }}>
        {
          thisTier.key + "=" + thisTier.value
        }
      </text>
    );
  }
}

interface LocalityBoxProps {
  projection: d3.geo.Projection;
  locality: LocalityTree;
  locationTree: LocationTree;
}

class LocalityBox extends React.Component<LocalityBoxProps, any> {
  render() {
    const location = findOrCalculateLocation(this.props.locationTree, this.props.locality);
    const center = this.props.projection([location.longitude, location.latitude]);
    const box = new Box(center[0] - 50, center[1] - 50, 100, 100);
    return (
      <g transform={`translate(${box.center()})`}>
        <LocalityView locality={this.props.locality} />
      </g>
    );
  }
}

interface NodeBoxProps {
  node: NodeStatus$Properties;
  nodeHistory: SimulatedNodeStatus;
}

class NodeBox extends React.Component<NodeBoxProps, any> {
  // TODO: layout!
  render() {
    return (
      <g>
        <NodeView
          nodeHistory={this.props.nodeHistory}
          maxClientActivityRate={10000}
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
          <LocalityBox projection={this.props.projection} locality={locality} locationTree={this.props.locationTree} />,
        );
      });
    });

    return children;
  }

  renderChildNodes(tree: LocalityTree) {
    const children: React.ReactNode[] = [];

    tree.nodes.forEach((node) => {
      const nodeHistory = this.props.nodeHistories[node.desc.node_id];

      children.push(
        <NodeBox node={node} nodeHistory={nodeHistory} />,
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
