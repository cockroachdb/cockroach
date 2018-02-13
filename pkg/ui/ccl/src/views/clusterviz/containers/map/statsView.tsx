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

const MAIN_BLUE = "#3A7DE1";
const DARK_BLUE = "#152849";

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
          {/* background */}
          <path
            fill={MAIN_BLUE}
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
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="34"
          fontWeight="bold"
          textAnchor="center"
          x="70"
          y="110"
        >
          {Math.round(capacityUsedPct)}%
        </text>
        <text
          fill="#152849"
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
          letterSpacing="1.333"
          transform="translate(15 8)"
          x="41"
          y="124"
        >
          CAPACITY
        </text>

        {/* labels at ends of arc */}
        <g fill={MAIN_BLUE}>
          <text x="17" y="156" textAnchor="center">
            {Bytes(usedCapacity)}
          </text>
          <text opacity=".65" x="118" y="156" textAnchor="center">
            {Bytes(capacity)}
          </text>
        </g>
      </g>
    );
  }

  renderLabel() {
    return (
      <g fontFamily="Lato-Bold, Lato" fontSize="14" fontWeight="bold" letterSpacing="1">
        <text fill={DARK_BLUE} x="50" y="22">
          {this.props.label}
        </text>
        <text fill={MAIN_BLUE} x="50" y="42">
          {this.props.subLabel}
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
          x="19"
          y="178"
        >
          CPU
        </text>
        <text
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          x="143"
          y="178"
        >
          XX%
        </text>
        <path fill={MAIN_BLUE} d="M56 169h69v10H56z" opacity=".35"/>
        <path fill={MAIN_BLUE} d="M56 172h54a2 2 0 0 1 0 4H56v-4z"/>
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
          x="20"
          y="197"
        >
          QPS
        </text>
        <text
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          x="138"
          y="197"
        >
          XXXX
        </text>
        {this.renderQPSSparkline()}
      </g>
    );
  }

  renderQPSSparkline() {
    // TODO(vilterp): replace this with the sparkline
    return (
      <g transform="translate(56 188)">
        <path fill={MAIN_BLUE} opacity=".35" d="M0 0h69v10H0z"/>
        <path
          stroke={MAIN_BLUE}
          strokeWidth="2"
          d="M-.838 4.29l5.819 3.355L10.984 9l4.429-3.04 5.311 1.685L26.178 2l5.397 7 5.334-3.04h10.656l6.037-.331L57.625 2l4.402 3.922 7.898-2.683"
        />
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
