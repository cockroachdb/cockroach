// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import PropTypes from "prop-types";
import React from "react";
import { InjectedRouter, RouterState } from "react-router";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { getChildLocalities, getLocality } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";

import { CircleLayout } from "./circleLayout";
import { MapLayout } from "./mapLayout";
import { NodeHistory } from "./nodeHistory";
import { LivenessStatus } from "src/redux/nodes";

interface ModalLocalitiesViewProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  tiers: LocalityTier[];
  nodeHistories: { [id: string]: NodeHistory };
  liveness: { [id: string]: LivenessStatus };
  viewportSize: [number, number];
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
      return <MapLayout
        localityTree={treeToRender}
        locationTree={this.props.locationTree}
        liveness={this.props.liveness}
        viewportSize={this.props.viewportSize}
      />;
    }

    return <CircleLayout
      localityTree={treeToRender}
      liveness={this.props.liveness}
      nodeHistories={this.props.nodeHistories}
      viewportSize={this.props.viewportSize}
    />;
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
