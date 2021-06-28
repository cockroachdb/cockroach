// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { CpuSparkline } from "src/views/clusterviz/containers/map/cpuSparkline";
import { QpsSparkline } from "src/views/clusterviz/containers/map/qpsSparkline";
import { DARK_BLUE } from "src/views/shared/colors";

const SPARKLINE_OFFSET_PX = 36;

interface SparklinesProps {
  nodes: string[];
}

export class Sparklines extends React.Component<SparklinesProps> {
  renderCPU() {
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
        {this.renderCPUSparkline()}
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
        {this.renderQPSSparkline()}
      </g>
    );
  }

  renderQPSSparkline() {
    return (
      <g transform={`translate(${SPARKLINE_OFFSET_PX} -9)`}>
        <QpsSparkline nodes={this.props.nodes} />
      </g>
    );
  }

  renderCPUSparkline() {
    return (
      <g transform={`translate(${SPARKLINE_OFFSET_PX} -9)`}>
        <CpuSparkline nodes={this.props.nodes} />
      </g>
    );
  }

  render() {
    return (
      <g transform="translate(20 178)" fill="none">
        {this.renderCPU()}
        {this.renderQPS()}
      </g>
    );
  }
}
