// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { BACKGROUND_BLUE, DARK_BLUE, MAIN_BLUE } from "ccl/src/views/shared/colors";

const STATS_BARS_WIDTH_PX = 69;
const STATS_BAR_OFFSET_PX = 36;

export class StatsBars extends React.Component {

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
        <rect
          x={STATS_BAR_OFFSET_PX}
          y="-9"
          width={STATS_BARS_WIDTH_PX}
          height="10"
          fill={BACKGROUND_BLUE}
        />
        <rect
          x={STATS_BAR_OFFSET_PX}
          y="-6"
          width="40"
          height="4"
          fill={MAIN_BLUE}
        />
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
      <g transform={`translate(${STATS_BAR_OFFSET_PX} -9)`}>
        <rect x="0" y="0" width={STATS_BARS_WIDTH_PX} height="10" fill={BACKGROUND_BLUE} />
        <path
          stroke={MAIN_BLUE}
          strokeWidth="2"
          d="M-.838 4.29l5.819 3.355L10.984 9l4.429-3.04 5.311 1.685L26.178 2l5.397 7 5.334-3.04h10.656l6.037-.331L57.625 2l4.402 3.922 7.898-2.683"
        />
      </g>
    );
  }

  render() {
    return (
      <g transform="translate(20 178)" fill="none">
        {this.renderCPUBar()}
        {this.renderQPS()}
      </g>
    );
  }

}
