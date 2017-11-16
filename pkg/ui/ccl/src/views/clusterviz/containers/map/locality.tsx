// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

import React from "react";

import { MetricConstants } from "src/util/proto";
import { Bytes } from "src/util/format";

import NodeStatusHistory from "./statusHistory";
import * as PathMath from "./pathmath";

export interface LocalityProps {
  nodeHistory: NodeStatusHistory;
  maxClientActivityRate: number;
}

export class Locality extends React.Component<LocalityProps, any> {
  static radius = 42;
  static arcWidth = Locality.radius * 0.11111;
  static outerRadius = Locality.radius + Locality.arcWidth;
  static maxRadius = Locality.outerRadius + Locality.arcWidth;

  renderBackground() {
    return (
      <path
        className="capacity-background"
        d={PathMath.createArcPath(
          Locality.radius, Locality.outerRadius, PathMath.arcAngleFromPct(0), PathMath.arcAngleFromPct(1),
        )}
      />
    );
  }

  renderCapacityArc() {
    // Compute used percentage.
    const { metrics } = this.props.nodeHistory.latest();
    const used = metrics[MetricConstants.usedCapacity];
    const capacity = metrics[MetricConstants.capacity];
    const capacityUsedPct = (capacity) ? used / capacity : 0;

    const usedX = Math.cos(PathMath.angleFromPct(capacityUsedPct));
    const usedY = Math.sin(PathMath.angleFromPct(capacityUsedPct));

    return (
      <g>
        <text
          className="capacity-label"
          x={(Locality.outerRadius + Locality.arcWidth) * Math.cos(0)}
        >
          {Bytes(capacity)}
        </text>
        <path
          className="capacity-used"
          d={PathMath.createArcPath(
            Locality.radius,
            Locality.outerRadius,
            PathMath.arcAngleFromPct(0),
            PathMath.arcAngleFromPct(capacityUsedPct),
          )}
        />
        <text
          className="capacity-used-label"
          transform={`translate(${usedX * Locality.maxRadius}, ${usedY * Locality.maxRadius})`}
          textAnchor={capacityUsedPct < 0.75 ? "end" : "start"}
        >
          {Bytes(used)}
        </text>

        <g transform={`translate(${-Locality.outerRadius}, ${-Locality.outerRadius})`}>
          <svg width={Locality.outerRadius * 2} height={Locality.outerRadius * 2}>
            <text className="capacity-used-pct-label" x="50%" y="40%">
              {Math.round(100 * capacityUsedPct) + "%"}
            </text>
            <text className="capacity-used-text" x="50%" y="60%">
              CAPACITY USED
              </text>
          </svg>
        </g>
      </g>
    );
  }

  renderNetworkActivity() {
    const barsX = Locality.radius * Math.cos(PathMath.angleFromPct(0));
    const barsWidth = Locality.outerRadius - barsX - 4;
    const labelH = 8;

    const { nodeHistory, maxClientActivityRate } = this.props;

    return (
      <g transform={`translate(0,${Locality.radius * Math.sin(PathMath.angleFromPct(0))})`} >
        <line
          className="client-activity"
          x1={Locality.outerRadius - 2}
          y1={-labelH}
          x2={Math.round(Locality.outerRadius - barsWidth * nodeHistory.clientActivityRate / maxClientActivityRate)}
          y2={-labelH}
        />
        <text className="client-activity-label" x={Locality.outerRadius + Locality.arcWidth} y={-labelH}>
          {Math.round(nodeHistory.clientActivityRate)} qps
          </text>
      </g>
    );
  }

  renderLabel() {
    return (
      <g transform={`translate(${-Locality.outerRadius}, ${Locality.outerRadius * 0.9})`}>
        <path
          className="locality-label-background"
          d={PathMath.drawBox(Locality.outerRadius * 2, 20, 0.05)}
        />
        <svg width={Locality.outerRadius * 2} height="20">
          <text className="locality-label" x="50%" y="55%">
            {this.props.nodeHistory.latest().desc.address.address_field}
          </text>
        </svg>
      </g>
    );
  }

  render() {
    return (
      <g className="locality">
        <g className="capacity-centric">
          {this.renderBackground()}
          {this.renderCapacityArc()}
          {this.renderNetworkActivity()}
          {this.renderLabel()}
        </g>
      </g>
    );
  }
}
