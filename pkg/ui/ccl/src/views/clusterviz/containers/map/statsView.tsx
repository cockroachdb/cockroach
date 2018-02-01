// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { Bytes } from "src/util/format";

import { SimulatedNodeStatus } from "./nodeSimulator";
import * as PathMath from "./pathmath";

interface StatsViewProps {
  capacity: number;
  usedCapacity: number;
  label: string;

  nodeHistory?: SimulatedNodeStatus;
  maxClientActivityRate?: number;
}

export class StatsView extends React.Component<StatsViewProps, any> {
  static radius = 42;
  static arcWidth = StatsView.radius * 0.11111;
  static outerRadius = StatsView.radius + StatsView.arcWidth;
  static maxRadius = StatsView.outerRadius + StatsView.arcWidth;

  renderBackground() {
    return (
      <path
        className="capacity-background"
        d={PathMath.createArcPath(
          StatsView.radius, StatsView.outerRadius, PathMath.arcAngleFromPct(0), PathMath.arcAngleFromPct(1),
        )}
      />
    );
  }

  renderCapacityArc() {
    // Compute used percentage.
    const usedCapacity = this.props.usedCapacity;
    const capacity = this.props.capacity;
    const capacityUsedPct = (capacity) ? usedCapacity / capacity : 0;

    const usedX = Math.cos(PathMath.angleFromPct(capacityUsedPct));
    const usedY = Math.sin(PathMath.angleFromPct(capacityUsedPct));

    return (
      <g>
        <text
          className="capacity-label"
          x={(StatsView.outerRadius + StatsView.arcWidth) * Math.cos(0)}
        >
          {Bytes(capacity)}
        </text>
        <path
          className="capacity-used"
          d={PathMath.createArcPath(
            StatsView.radius,
            StatsView.outerRadius,
            PathMath.arcAngleFromPct(0),
            PathMath.arcAngleFromPct(capacityUsedPct),
          )}
        />
        <text
          className="capacity-used-label"
          transform={`translate(${usedX * StatsView.maxRadius}, ${usedY * StatsView.maxRadius})`}
          textAnchor={capacityUsedPct < 0.75 ? "end" : "start"}
        >
          {Bytes(usedCapacity)}
        </text>

        <g transform={`translate(${-StatsView.outerRadius}, ${-StatsView.outerRadius})`}>
          <svg width={StatsView.outerRadius * 2} height={StatsView.outerRadius * 2}>
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
    const barsX = StatsView.radius * Math.cos(PathMath.angleFromPct(0));
    const barsWidth = StatsView.outerRadius - barsX - 4;
    const labelH = 8;

    const { nodeHistory, maxClientActivityRate } = this.props;

    return (
      <g transform={`translate(0,${StatsView.radius * Math.sin(PathMath.angleFromPct(0))})`} >
        <line
          className="client-activity"
          x1={StatsView.outerRadius - 2}
          y1={-labelH}
          x2={Math.round(StatsView.outerRadius - barsWidth * nodeHistory.clientActivityRate / maxClientActivityRate)}
          y2={-labelH}
        />
        <text className="client-activity-label" x={StatsView.outerRadius + StatsView.arcWidth} y={-labelH}>
          {Math.round(nodeHistory.clientActivityRate)} qps
        </text>
      </g>
    );
  }

  renderLabel() {
    return (
      <g transform={`translate(${-StatsView.outerRadius}, ${StatsView.outerRadius * 0.9})`}>
        <path
          className="locality-label-background"
          d={PathMath.drawBox(StatsView.outerRadius * 2, 20, 0.05)}
        />
        <svg width={StatsView.outerRadius * 2} height="20">
          <text className="locality-label" x="50%" y="55%">
            {this.props.label}
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
          {this.props.nodeHistory ? this.renderNetworkActivity() : null}
          {this.renderLabel()}
        </g>
      </g>
    );
  }
}
