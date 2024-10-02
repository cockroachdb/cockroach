// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as d3 from "d3";
import React from "react";

import shapes from "./world.json";

interface WorldMapProps {
  projection: d3.geo.Projection;
}

export class WorldMap extends React.Component<WorldMapProps> {
  render() {
    const pathGen = d3.geo.path().projection(this.props.projection);
    return (
      <g>
        <g>
          {shapes.features.map((feature: any, i: number) => (
            <path
              key={i}
              className="geopath"
              id={`world-${feature.id}`}
              d={pathGen(feature)}
            />
          ))}
        </g>
      </g>
    );
  }
}
