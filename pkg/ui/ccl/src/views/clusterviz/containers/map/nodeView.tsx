// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { MetricConstants } from "src/util/proto";
import { Bytes } from "src/util/format";

import { SimulatedNodeStatus } from "./nodeSimulator";
import * as PathMath from "./pathmath";

interface NodeViewProps {
  nodeHistory: SimulatedNodeStatus;
  maxClientActivityRate: number;
}

export class NodeView extends React.Component<NodeViewProps, any> {
  static radius = 42;
  static arcWidth = NodeView.radius * 0.11111;
  static outerRadius = NodeView.radius + NodeView.arcWidth;
  static maxRadius = NodeView.outerRadius + NodeView.arcWidth;

  renderBackground() {
    return (
      <path
        className="capacity-background"
        d={PathMath.createArcPath(
          NodeView.radius, NodeView.outerRadius, PathMath.arcAngleFromPct(0), PathMath.arcAngleFromPct(1),
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
          x={(NodeView.outerRadius + NodeView.arcWidth) * Math.cos(0)}
        >
          {Bytes(capacity)}
        </text>
        <path
          className="capacity-used"
          d={PathMath.createArcPath(
            NodeView.radius,
            NodeView.outerRadius,
            PathMath.arcAngleFromPct(0),
            PathMath.arcAngleFromPct(capacityUsedPct),
          )}
        />
        <text
          className="capacity-used-label"
          transform={`translate(${usedX * NodeView.maxRadius}, ${usedY * NodeView.maxRadius})`}
          textAnchor={capacityUsedPct < 0.75 ? "end" : "start"}
        >
          {Bytes(used)}
        </text>

        <g transform={`translate(${-NodeView.outerRadius}, ${-NodeView.outerRadius})`}>
          <svg width={NodeView.outerRadius * 2} height={NodeView.outerRadius * 2}>
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
    const barsX = NodeView.radius * Math.cos(PathMath.angleFromPct(0));
    const barsWidth = NodeView.outerRadius - barsX - 4;
    const labelH = 8;

    const { nodeHistory, maxClientActivityRate } = this.props;

    return (
      <g transform={`translate(0,${NodeView.radius * Math.sin(PathMath.angleFromPct(0))})`} >
        <line
          className="client-activity"
          x1={NodeView.outerRadius - 2}
          y1={-labelH}
          x2={Math.round(NodeView.outerRadius - barsWidth * nodeHistory.clientActivityRate / maxClientActivityRate)}
          y2={-labelH}
        />
        <text className="client-activity-label" x={NodeView.outerRadius + NodeView.arcWidth} y={-labelH}>
          {Math.round(nodeHistory.clientActivityRate)} qps
          </text>
      </g>
    );
  }

  renderLabel() {
    return (
      <g transform={`translate(${-NodeView.outerRadius}, ${NodeView.outerRadius * 0.9})`}>
        <path
          className="locality-label-background"
          d={PathMath.drawBox(NodeView.outerRadius * 2, 20, 0.05)}
        />
        <svg width={NodeView.outerRadius * 2} height="20">
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
