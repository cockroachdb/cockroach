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
import { generateLocalityRoute, getChildLocalities, getLocality } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { NodeView } from "./nodeView";
import { ZoomTransformer } from "./zoom";

const MIN_RADIUS = 150;
const PADDING = 150;

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

interface ModalLocalitiesViewProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  tiers: LocalityTier[];
  nodeHistories: { [id: string]: SimulatedNodeStatus };
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
}

class MapLayout extends React.Component<ModalLocalitiesViewProps, any> {
  renderChildLocalities() {
    return getChildLocalities(this.props.localityTree).map((locality) => {
      const location = findOrCalculateLocation(this.props.locationTree, locality);
      const center = this.props.projection([location.longitude, location.latitude]);

      return (
        <g transform={`translate(${center})`}>
          <LocalityView locality={locality} />
        </g>
      );
    });
  }

  render() {
    return (
      <g>
        { this.renderChildLocalities() }
      </g>
    );
  }
}

class CircleLayout extends React.Component<ModalLocalitiesViewProps, any> {
  coordsFor(index: number, total: number, radius: number) {
    const angle = 2 * Math.PI * index / total - Math.PI / 2;
    return [radius * Math.cos(angle), radius * Math.sin(angle)];
  }

  render() {
    const { localityTree } = this.props;
    const childLocalities = getChildLocalities(localityTree);

    const total = localityTree.nodes.length + childLocalities.length;

    const viewport = this.props.zoom.viewportSize();
    const calculatedRadius = Math.min(...viewport) / 2 - PADDING;
    const radius = Math.max(MIN_RADIUS, calculatedRadius);

    return (
      <g transform={`translate(${viewport[0] / 2},${viewport[1] / 2})`}>
        {
          childLocalities.map((locality, i) => (
            <g transform={`translate(${this.coordsFor(i, total, radius)})`}>
              <LocalityView locality={locality} />
            </g>
          ))
        }
        {
          localityTree.nodes.map((node, i) => {
            const nodeHistory = this.props.nodeHistories[node.desc.node_id];

            return (
              <g transform={`translate(${this.coordsFor(i + childLocalities.length, total, radius)})`}>
                <NodeView
                  nodeHistory={nodeHistory}
                  maxClientActivityRate={10000}
                />
              </g>
            );
          })
        }
      </g>
    );
  }
}

export class ModalLocalitiesView extends React.Component<ModalLocalitiesViewProps, any> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  render() {
    const treeToRender = getLocality(this.props.localityTree, this.props.tiers);
    if (_.isNil(treeToRender)) {
      this.context.router.replace(CLUSTERVIZ_ROOT);
    }

    if (renderAsMap(this.props.locationTree, treeToRender)) {
      return <MapLayout {...this.props} localityTree={treeToRender} />;
    }

    return <CircleLayout {...this.props} localityTree={treeToRender} />;
  }
}

// Only exported for test purposes.
export function renderAsMap(locationTree: LocationTree, localityTree: LocalityTree) {
  // If there are any nodes directly under this locality, don't show a map.
  if (!_.isEmpty(localityTree.nodes)) {
    return false;
  }

  // Otherwise, show a map as long as we're able to find or calculate a location
  // for every child locality.
  const children = getChildLocalities(localityTree);
  return _.every(
    children,
    (child) => !_.isNil(findOrCalculateLocation(locationTree, child)),
  );
}
