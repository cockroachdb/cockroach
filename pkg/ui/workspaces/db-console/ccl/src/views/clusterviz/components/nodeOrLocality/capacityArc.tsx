// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import React from "react";

import {
  NodeArcPercentageTooltip,
  NodeArcUsedCapacityTooltip,
  NodeArcTotalCapacityTooltip,
} from "src/views/clusterviz/components/nodeOrLocality/tooltips";
import * as PathMath from "src/views/clusterviz/util/pathmath";
import {
  BACKGROUND_BLUE,
  DARK_BLUE,
  LIGHT_TEXT_BLUE,
  MAIN_BLUE,
} from "src/views/shared/colors";

const ARC_INNER_RADIUS = 56;
const ARC_WIDTH = 6;
const ARC_OUTER_RADIUS = ARC_INNER_RADIUS + ARC_WIDTH;

interface CapacityArcProps {
  usedCapacity: number;
  usableCapacity: number;
  nodeLabel?: string;
  localityLabel?: string;
}

export class CapacityArc extends React.Component<CapacityArcProps> {
  render() {
    const { Bytes } = util;
    // Compute used percentage.
    const usedCapacity = this.props.usedCapacity;
    const capacity = this.props.usableCapacity;
    const capacityUsedPct = capacity ? (usedCapacity / capacity) * 100 : 0;

    return (
      <g>
        <g transform="translate(90 115)">
          {/* background arc */}
          <path
            fill={BACKGROUND_BLUE}
            strokeLinecap="round"
            d={PathMath.createArcPath(
              ARC_INNER_RADIUS,
              ARC_OUTER_RADIUS,
              PathMath.arcAngleFromPct(0),
              PathMath.arcAngleFromPct(1),
              ARC_WIDTH,
            )}
          />
          {/* current value arc */}
          <path
            fill={MAIN_BLUE}
            strokeLinecap="round"
            d={PathMath.createArcPath(
              ARC_INNER_RADIUS,
              ARC_OUTER_RADIUS,
              PathMath.arcAngleFromPct(0),
              PathMath.arcAngleFromPct(capacityUsedPct / 100),
              ARC_WIDTH,
            )}
          />
        </g>

        {/* text inside arc */}
        <text
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="34"
          fontWeight="bold"
          textAnchor="middle"
          x="90"
          y="110"
        >
          {Math.round(capacityUsedPct)}%
        </text>
        <NodeArcPercentageTooltip {...this.props}>
          <text
            fill={DARK_BLUE}
            fontFamily="Lato-Bold, Lato"
            fontSize="12"
            fontWeight="bold"
            letterSpacing="1.333"
            textAnchor="middle"
            x="90"
            y="132"
          >
            CAPACITY
          </text>
        </NodeArcPercentageTooltip>

        {/* labels at ends of arc */}
        <NodeArcUsedCapacityTooltip {...this.props}>
          <text fill={MAIN_BLUE} x="17" y="156" textAnchor="center">
            {Bytes(usedCapacity)}
          </text>
        </NodeArcUsedCapacityTooltip>
        <NodeArcTotalCapacityTooltip {...this.props}>
          <text fill={LIGHT_TEXT_BLUE} x="118" y="156" textAnchor="center">
            {Bytes(capacity)}
          </text>
        </NodeArcTotalCapacityTooltip>
      </g>
    );
  }
}
