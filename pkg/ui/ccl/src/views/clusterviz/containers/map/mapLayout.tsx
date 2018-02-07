// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import * as d3 from "d3";
import React from "react";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getChildLocalities } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";

import { LocalityView } from "./localityView";
import { WorldMap } from "./worldmap";
import { ZoomTransformer } from "./zoom";
import { LivenessStatus } from "src/redux/nodes";

interface MapLayoutProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  liveness: { [id: string]: LivenessStatus };
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
}

export class MapLayout extends React.Component<MapLayoutProps, any> {
  renderChildLocalities() {
    return getChildLocalities(this.props.localityTree).map((locality) => {
      const location = findOrCalculateLocation(this.props.locationTree, locality);
      const center = this.props.projection([location.longitude, location.latitude]);

      return (
        <g transform={`translate(${center})`}>
          <LocalityView localityTree={locality} liveness={this.props.liveness} />
        </g>
      );
    });
  }

  render() {
    const viewportSize = this.props.zoom.viewportSize();

    return (
      <g>
        <rect width={viewportSize[0]} height={viewportSize[1]} fill="lavender" />
        <WorldMap projection={this.props.projection} />
        { this.renderChildLocalities() }
      </g>
    );
  }
}
