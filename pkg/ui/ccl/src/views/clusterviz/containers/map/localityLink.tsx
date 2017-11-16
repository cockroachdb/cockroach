// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

import React from "react";
import * as d3 from "d3";

export interface LocalityLinkProps {
  id: string;
  reversed: boolean;
  points: [number, number][];
}

export class LocalityLink extends React.Component<LocalityLinkProps, any> {
  render() {
    const data = this.props;
    const pathId = `${data.id}-path`;
    return (
      <g className="locality-link-group">
        <path
          className="locality-link"
          id={pathId}
          d={d3.svg.line().interpolate("cardinal-open").tension(0.5)(data.points)}
        />
        <text>
          <textPath
            className="incoming-throughput-label"
            startOffset="50%"
            xlinkHref={`#${pathId}`}
          >
            in
          </textPath>
        </text>
        <text>
          <textPath
            className="outgoing-throughput-label"
            startOffset="50%"
            xlinkHref={`#${pathId}`}
          >
            out
          </textPath>
        </text>
        <text>
          <textPath
            className="rtt-label"
            startOffset="60%"
            xlinkHref={`#${pathId}`}
          >
            rtt
          </textPath>
        </text>
      </g>
    );
  }
}
