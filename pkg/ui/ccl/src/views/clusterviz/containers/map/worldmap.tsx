// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

/// <reference path="./typings.d.ts" />

import React from "react";
import * as d3 from "d3";

import worldShapes from "./world.json";
import usStateShapes from "./us-states.json";

interface WorldMapProps {
  projection: d3.geo.Projection;
}

export class WorldMap extends React.Component<WorldMapProps> {
  render() {
    const pathGen = d3.geo.path().projection(this.props.projection);
    return (
      <g>
        <g>
          {worldShapes.features.map((feature: any, i: number) =>
            <path
              key={i}
              className="geopath"
              id={`world-${feature.id}`}
              d={pathGen(feature)}
            />,
          )}
        </g>
        <g>
          {usStateShapes.features.map((feature: any, i: number) =>
            <path
              key={i}
              className="geopath"
              d={pathGen(feature)}
            />,
          )}
        </g>
      </g>
    );
  }
}
