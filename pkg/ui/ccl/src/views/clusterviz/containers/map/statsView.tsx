// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { Bytes } from "src/util/format";
import { NodeHistory } from "./nodeHistory";
import * as PathMath from "./pathmath";
import localityIcon from "!!raw-loader!assets/localityIcon.svg";
import nodeIcon from "!!raw-loader!assets/nodeIcon.svg";
import { trustIcon } from "src/util/trust";

interface StatsViewProps {
  usableCapacity: number;
  usedCapacity: number;
  label: string;
  subLabel: string; // shows up under the label
  isLocality: boolean;

  nodeHistory?: NodeHistory;
  maxClientActivityRate?: number;
}

const ARC_INNER_RADIUS = 56;
const ARC_WIDTH = 6;
const ARC_OUTER_RADIUS = ARC_INNER_RADIUS + ARC_WIDTH;

export class StatsView extends React.Component<StatsViewProps> {
  renderCapacityArc() {
    // Compute used percentage.
    const usedCapacity = this.props.usedCapacity;
    const capacity = this.props.usableCapacity;
    const capacityUsedPct = capacity ? (usedCapacity / capacity * 100) : 0;

    return (
      <g>
        <g transform="translate(90 115)">
          {/* current value */}
          <path
            fill="#3A7DE1"
            strokeLinecap="round"
            d={PathMath.createArcPath(
              ARC_INNER_RADIUS,
              ARC_OUTER_RADIUS,
              PathMath.arcAngleFromPct(0),
              PathMath.arcAngleFromPct(capacityUsedPct / 100),
              ARC_WIDTH,
            )}
          />
          {/* background */}
          <path
            fill="#3A7DE1"
            strokeLinecap="round"
            opacity={0.35}
            d={PathMath.createArcPath(
              ARC_INNER_RADIUS,
              ARC_OUTER_RADIUS,
              PathMath.arcAngleFromPct(0),
              PathMath.arcAngleFromPct(1),
              ARC_WIDTH,
            )}
          />
        </g>

        {/* text inside arc */}
        <text
          fill="#3A7DE1"
          fontFamily="Lato-Bold, Lato"
          fontSize="34"
          fontWeight="bold"
          transform="translate(83 8)"
          textAnchor="end"
        >
          <tspan x="41.129" y="105">{Math.round(capacityUsedPct)}%</tspan>
        </text>
        <text
          fill="#152849"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
          letterSpacing="1.333"
          transform="translate(15 8)"
        >
          <tspan x="41.088" y="124">CAPACITY</tspan>
        </text>

        {/* labels at ends of arc */}
        <g fill="#3A7DE1">
          <text transform="translate(21 144)">
            <tspan x=".194" y="12">{Bytes(usedCapacity)}</tspan>
          </text>
          <text opacity=".65" transform="translate(21 144)">
            <tspan x="97.194" y="13">{Bytes(capacity)}</tspan>
          </text>
        </g>
      </g>
    );
  }

  renderLabel() {
    return (
      <g fontFamily="Lato-Bold, Lato" fontSize="14" fontWeight="bold" letterSpacing="1">
        <text fill="#152849" transform="translate(50 8)">
          <tspan x=".17" y="14">{this.props.label}</tspan>
        </text>
        <text fill="#3A7DE1" transform="translate(50 8)">
          <tspan x=".17" y="34">{this.props.subLabel}</tspan>
        </text>
      </g>
    );
  }

  renderCPUBar() {
    return (
      <g>
        <text
          fill="#152849"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
          transform="translate(15 8)"
        >
          <tspan x="4.718" y="170">CPU</tspan>
        </text>
        <text
          fill="#3A7DE1"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          transform="translate(15 8)"
        >
          <tspan x="123" y="170">XX%</tspan>
        </text>
        <path fill="#3A7DE1" d="M56 169h69v10H56z" opacity=".35"/>
        <path fill="#3A7DE1" d="M56 172h54a2 2 0 0 1 0 4H56v-4z"/>
      </g>
    );
  }

  renderQPS() {
    return (
      <g>
        <text
          fill="#152849"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
          transform="translate(15 8)"
        >
          <tspan x="5.468" y="189">QPS</tspan>
        </text>
        <text
          fill="#3A7DE1"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          transform="translate(15 8)"
        >
          <tspan x="123" y="189">XXXX</tspan>
        </text>
        {/* TODO(vilterp): replace this with the sparkline */}
        <g transform="translate(56 188)">
          <mask fill="#fff">
            <path d="M0 0h69v10H0z"/>
          </mask>
          <path fill="#3A7DE1" opacity=".35" d="M0 0h69v10H0z"/>
          <path
            stroke="#3A7DE1"
            strokeWidth="2"
            d="M-.838 4.29l5.819 3.355L10.984 9l4.429-3.04 5.311 1.685L26.178 2l5.397 7 5.334-3.04h10.656l6.037-.331L57.625 2l4.402 3.922 7.898-2.683"
            mask="url(#e)"
          />
        </g>
      </g>
    );
  }

  renderLiveCheckmark() {
    return (
      <g>
        <g transform="translate(5 5)">
          <circle fill="#54B30E" cx="8" cy="8" r="8"/>
          <circle cx="8" cy="8" r="8.5" stroke="#FFF"/>
        </g>
        <path
          stroke="#FFF"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3" d="M8.81 13.284l2.878 2.879L16.84 9.57"
        />
      </g>
    );
  }

  renderLocalityIcon() {
    return (
      <g dangerouslySetInnerHTML={trustIcon(localityIcon)} transform="translate(14 14)" />
    );
  }

  renderNodeIcon() {
    return (
      <g dangerouslySetInnerHTML={trustIcon(nodeIcon)} transform="translate(14 14)" />
    );
  }

  render() {
    return (
      // TODO(vilterp): surprisingly, it doesn't render correctly without the fill: none.
      // would like to remove this; need to figure out what's going on.
      <g fill="none" transform="translate(-90 -100)">
        {this.renderCapacityArc()}
        {this.renderLabel()}
        {this.renderCPUBar()}
        {this.renderQPS()}
        {this.props.isLocality ? this.renderLocalityIcon() : this.renderNodeIcon()}
        {this.renderLiveCheckmark()}
      </g>
    );
  }

}
