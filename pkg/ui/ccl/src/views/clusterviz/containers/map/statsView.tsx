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
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
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
const BACKGROUND_BLUE = "#B8CCEC";
const LIGHT_TEXT_BLUE = "#85A7E3";
const DARK_BLUE = "#152849";

const STATS_BARS_WIDTH_PX = 69;

export class StatsView extends React.Component<StatsViewProps> {
  renderCapacityArc() {
    // Compute used percentage.
    const usedCapacity = this.props.usedCapacity;
    const capacity = this.props.usableCapacity;
    const capacityUsedPct = capacity ? (usedCapacity / capacity * 100) : 0;

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

        {/* labels at ends of arc */}
        <text fill={MAIN_BLUE} x="17" y="156" textAnchor="center">
          {Bytes(usedCapacity)}
        </text>
        <text fill={LIGHT_TEXT_BLUE} x="118" y="156" textAnchor="center">
          {Bytes(capacity)}
        </text>
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
          fill={DARK_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
        >
          CPU
        </text>
        <text
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          x="124"
        >
          XX%
        </text>
        <rect x="37" y="-9" width={STATS_BARS_WIDTH_PX} height="10" fill={BACKGROUND_BLUE} />
        <rect x="37" y="-6" width="40" height="4" fill={MAIN_BLUE} />
      </g>
    );
  }

  renderQPS() {
    return (
      <g transform="translate(0 19)">
        <text
          fill={DARK_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="bold"
        >
          QPS
        </text>
        <text
          fill={MAIN_BLUE}
          fontFamily="Lato-Bold, Lato"
          fontSize="12"
          fontWeight="700"
          x="118"
        >
          XXXX
        </text>
        {this.renderQPSSparkline()}
      </g>
    );
  }

  renderQPSSparkline() {
    // TODO(vilterp): replace this with the real sparkline
    return (
      <g transform="translate(36 -9)">
        <rect x="0" y="0" width={STATS_BARS_WIDTH_PX} height="10" fill={BACKGROUND_BLUE} />
        <path
          stroke={MAIN_BLUE}
          strokeWidth="2"
          d="M-.838 4.29l5.819 3.355L10.984 9l4.429-3.04 5.311 1.685L26.178 2l5.397 7 5.334-3.04h10.656l6.037-.331L57.625 2l4.402 3.922 7.898-2.683"
        />
      </g>
    );
  }

  renderStatsUnderArc() {
    return (
      <g transform="translate(20 178)">
        {this.renderCPUBar()}
        {this.renderQPS()}
      </g>
    );
  }

  renderLivenessIcon() {
    // TODO(vilterp): pipe in real liveness data; add other icons
    return (
      <g dangerouslySetInnerHTML={trustIcon(liveIcon)} />
    );
  }

  renderLocalityOrNodeIcon() {
    const icon = this.props.isLocality
      ? <g dangerouslySetInnerHTML={trustIcon(localityIcon)} />
      : <g dangerouslySetInnerHTML={trustIcon(nodeIcon)} />;
    return (
      <g transform="translate(14 14)">{icon}</g>
    );
  }

  render() {
    return (
      // TODO(vilterp): surprisingly, it doesn't render correctly without the fill: none.
      // would like to remove this; need to figure out what's going on.
      <g fill="none" transform="translate(-90 -100)">
        {this.renderLabel()}
        {this.renderLocalityOrNodeIcon()}
        {this.renderLivenessIcon()}
        {this.renderCapacityArc()}
        {this.renderStatsUnderArc()}
      </g>
    );
  }

}
