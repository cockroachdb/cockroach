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
  usableCapacity: number;
  usedCapacity: number;
  label: string;
  subLabel: string; // shows up under the label

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
    const capacity = this.props.usableCapacity;
    const capacityUsedPct = (capacity) ? usedCapacity / capacity : 0;

    // const usedX = Math.cos(PathMath.angleFromPct(capacityUsedPct));
    // const usedY = Math.sin(PathMath.angleFromPct(capacityUsedPct));
    //
    // return (
    //   <g>
    //     <text
    //       className="capacity-label"
    //       x={(StatsView.outerRadius + StatsView.arcWidth) * Math.cos(0)}
    //     >
    //       {Bytes(capacity)}
    //     </text>
    //     <path
    //       className="capacity-used"
    //       d={PathMath.createArcPath(
    //         StatsView.radius,
    //         StatsView.outerRadius,
    //         PathMath.arcAngleFromPct(0),
    //         PathMath.arcAngleFromPct(capacityUsedPct),
    //       )}
    //     />
    //     <text
    //       className="capacity-used-label"
    //       transform={`translate(${usedX * StatsView.maxRadius}, ${usedY * StatsView.maxRadius})`}
    //       textAnchor={capacityUsedPct < 0.75 ? "end" : "start"}
    //     >
    //       {Bytes(usedCapacity)}
    //     </text>
    //
    //     <g transform={`translate(${-StatsView.outerRadius}, ${-StatsView.outerRadius})`}>
    //       <svg width={StatsView.outerRadius * 2} height={StatsView.outerRadius * 2}>
    //         <text className="capacity-used-pct-label" x="50%" y="40%">
    //           {Math.round(100 * capacityUsedPct) + "%"}
    //         </text>
    //         <text className="capacity-used-text" x="50%" y="60%">
    //           CAPACITY USED
    //         </text>
    //       </svg>
    //     </g>
    //   </g>
    // );

    // const usedX = Math.cos(PathMath.angleFromPct(capacityUsedPct));
    // const usedY = Math.sin(PathMath.angleFromPct(capacityUsedPct));

    return (
      <g>
        <path stroke="#3A7DE1" strokeLinecap="round" strokeWidth="6" d="M36.244 137.47c-5.536-15.686-4.138-33.656 5.363-48.86 16.563-26.506 51.478-34.566 77.986-18.002 23.374 14.606 32.406 43.48 22.86 68.276" opacity=".35"/>
        <path stroke="#3A7DE1" strokeLinecap="round" strokeWidth="6" d="M36.244 137.47c-5.536-15.686-4.138-33.656 5.363-48.86 16.563-26.506 51.478-34.566 77.986-18.002"/>

        {/* text inside arc */}
        <text fill="#3A7DE1" fontFamily="Lato-Bold, Lato" fontSize="34" fontWeight="bold" transform="translate(83 8)" textAnchor="end">
          <tspan x="41.129" y="105">{Math.round(capacityUsedPct)}%</tspan>
        </text>
        <text fill="#152849" fontFamily="Lato-Bold, Lato" fontSize="12" fontWeight="bold" letterSpacing="1.333" transform="translate(15 8)">
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
        <text fill="#152849" fontFamily="Lato-Bold, Lato" fontSize="12" fontWeight="bold" transform="translate(15 8)">
          <tspan x="4.718" y="170">CPU</tspan>
        </text>
        <text fill="#3A7DE1" fontFamily="Lato-Black, Lato" fontSize="12" fontWeight="700" transform="translate(15 8)">
          <tspan x="123" y="170">85%</tspan>
        </text>
        <path fill="#3A7DE1" d="M56 169h69v10H56z" opacity=".35"/>
        <path fill="#3A7DE1" d="M56 172h54a2 2 0 0 1 0 4H56v-4z"/>
      </g>
    );
  }

  renderQPS() {
    return (
      <g>
        <text fill="#3A7DE1" fontFamily="Lato-Black, Lato" fontSize="12" fontWeight="700" transform="translate(15 8)">
          <tspan x="123" y="189">1342</tspan>
        </text>
        <text fill="#152849" fontFamily="Lato-Bold, Lato" fontSize="12" fontWeight="bold" transform="translate(15 8)">
          <tspan x="5.468" y="189">QPS</tspan>
        </text>
        <g transform="translate(56 188)">
          <mask id="e" fill="#fff">
            <path d="M0 0h69v10H0z"/>
          </mask>
          <path fill="#3A7DE1" opacity=".35" id="d" d="M0 0h69v10H0z"/>
          <path stroke="#3A7DE1" strokeWidth="2" d="M-.838 4.29l5.819 3.355L10.984 9l4.429-3.04 5.311 1.685L26.178 2l5.397 7 5.334-3.04h10.656l6.037-.331L57.625 2l4.402 3.922 7.898-2.683" mask="url(#e)"/>
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
        <path stroke="#FFF" strokeLinecap="round" strokeLinejoin="round" strokeWidth="3" d="M8.81 13.284l2.878 2.879L16.84 9.57"/>
      </g>
    );
  }

  render() {
    return (
      <g fill="none" fillRule="evenodd">
        {this.renderCapacityArc()}
        {this.renderLabel()}
        {this.renderCPUBar()}
        {this.renderQPS()}
        {localityIcon}
        {this.renderLiveCheckmark()}
      </g>
    );
  }

}

const nodeIcon = (
  <path id="a" d="M14.163 14.086a.901.901 0 0 1 .004.08c0 1.381-3.172 2.5-7.084 2.5-3.912 0-7.083-1.119-7.083-2.5 0-.026.001-.053.004-.08A1.013 1.013 0 0 1 0 14V2.667c0-.03.001-.058.004-.086A.901.901 0 0 1 0 2.5C0 1.12 3.171 0 7.083 0c3.912 0 7.084 1.12 7.084 2.5a.901.901 0 0 1-.004.08c.002.03.004.058.004.087V14c0 .029-.002.058-.004.086z" />
);

const localityIcon = (
  <g>
    <g strokeLinecap="round" strokeLinejoin="round" transform="matrix(-1 0 0 1 36.667 17)">
      <g fill="#3A7DE1">{nodeIcon}</g>
      <path stroke="#FFF" d="M14.664 14.086c.002.024.002.038.003.08 0 1.8-3.405 3-7.584 3s-7.583-1.2-7.583-3c0-.062 0-.062.005-.038A1.513 1.513 0 0 1-.5 14V2.667c0-.043.002-.086.006-.041C-.5 2.563-.5 2.563-.5 2.5c0-1.798 3.404-3 7.583-3 4.18 0 7.584 1.202 7.584 3-.001.063-.001.063-.006.038.004.043.006.086.006.129V14c0 .029-.001.057-.003.086z"/>
    </g>
    <path stroke="#FFF" strokeWidth="1.03" d="M37.182 20.652H21.985v.515c0 1.81 3.411 3.015 7.598 3.015 4.187 0 7.599-1.204 7.599-3.015v-.515zM37.182 25.652H21.985v.515c0 1.81 3.411 3.015 7.598 3.015 4.187 0 7.599-1.204 7.599-3.015v-.515z"/>
    <g strokeLinecap="round" strokeLinejoin="round" transform="matrix(-1 0 0 1 30 22)">
      <g fill="#3A7DE1">{nodeIcon}</g>
      <path stroke="#FFF" d="M14.664 14.086c.002.024.002.038.003.08 0 1.8-3.405 3-7.584 3s-7.583-1.2-7.583-3c0-.062 0-.062.005-.038A1.513 1.513 0 0 1-.5 14V2.667c0-.043.002-.086.006-.041C-.5 2.563-.5 2.563-.5 2.5c0-1.798 3.404-3 7.583-3 4.18 0 7.584 1.202 7.584 3-.001.063-.001.063-.006.038.004.043.006.086.006.129V14c0 .029-.001.057-.003.086z"/>
    </g>
    <path stroke="#FFF" strokeWidth="1.03" d="M30.515 25.652H15.318v.515c0 1.81 3.412 3.015 7.599 3.015s7.598-1.204 7.598-3.015v-.515zM30.515 30.652H15.318v.515c0 1.81 3.412 3.015 7.599 3.015s7.598-1.204 7.598-3.015v-.515z"/>
    <g strokeLinecap="round" strokeLinejoin="round" transform="matrix(-1 0 0 1 40 25.333)">
      <g fill="#3A7DE1">{nodeIcon}</g>
      <path stroke="#FFF" d="M14.664 14.086c.002.024.002.038.003.08 0 1.8-3.405 3-7.584 3s-7.583-1.2-7.583-3c0-.062 0-.062.005-.038A1.513 1.513 0 0 1-.5 14V2.667c0-.043.002-.086.006-.041C-.5 2.563-.5 2.563-.5 2.5c0-1.798 3.404-3 7.583-3 4.18 0 7.584 1.202 7.584 3-.001.063-.001.063-.006.038.004.043.006.086.006.129V14c0 .029-.001.057-.003.086z"/>
    </g>
    <path stroke="#FFF" strokeWidth="1.03" d="M40.515 28.985H25.318v.515c0 1.811 3.412 3.015 7.599 3.015s7.598-1.204 7.598-3.015v-.515zM40.515 33.985H25.318v.515c0 1.811 3.412 3.015 7.599 3.015s7.598-1.204 7.598-3.015v-.515z"/>
  </g>
);
