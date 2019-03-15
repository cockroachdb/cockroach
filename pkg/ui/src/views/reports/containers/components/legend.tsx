// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import d3 from "d3";
import * as React from "react";

import * as protos from  "src/js/protos";
import createChartComponent from "src/views/shared/util/d3-react";
import { ComponentActivityMetricsI, ComponentActivityMetrics } from "./component_activity";
import "./legend.styl";

export const errorColors: string[] = ["#00ff00","#36cc00","#91cc00","#eccc00","#ffb000","#ff4e00","#ff0000"];

interface LegendProps {
  width: number;
  metrics: ComponentActivityMetricsI;
}

function countLegend(axis) {
  return function render(sel: d3.Selection<LegendProps>) {
    const { props } = sel.datum();

    sel.attr("class", "legend");

    const gradients = sel.selectAll("defs")
      .data(errorColors);
    gradients.enter()
      .append("defs")
      .append("svg:linearGradient")
      .attr("id", (d, idx) => "gradient-" + idx)
      .attr("x1", "0%")
      .attr("y1", "100%")
      .attr("x2", "100%")
      .attr("y2", "100%")
      .attr("spreadMethod", "pad");

    errorColors.forEach((col, idx) => {
      const stops = sel.select("#gradient-" + idx).selectAll("stop")
        .data([{offset: 0, color: col, opacity: 0.1}, {offset: 100, color: col, opacity: 1}]);
      stops.enter()
        .append("stop")
        .attr("offset", d => d.offset + "%")
        .attr("stop-color", d => d.color)
        .attr("stop-opacity", d => d.opacity);
    });

    const left = 60;
    const legendBG = sel.selectAll("rect")
      .data(errorColors);
    legendBG.enter()
      .append("rect")
      .attr("fill", (d, idx) => { return "url(#gradient-" + idx + ")"; })
      .attr("fill-opacity", 1)
      .attr("stroke", "none")
      .attr("x", left)
      .attr("y", (d, idx) => { return 13 + 2*idx; })
      .attr("height", "2");
    legendBG.attr("width", props.width);

    const legendName = sel.selectAll("text")
      .data([props]);
    legendName.enter()
      .append("text")
      .attr("class", "name")
      .attr("transform", "translate(0, 20)")
      .text("Velocity");

    const domain = [0, props.metrics.maxSpanCount];
    const scale = d3.scale.linear().range([0, props.width]).domain(domain);
    axis.scale(scale)
      .tickSize(2, 2)
      .ticks(11)
      .tickFormat(d => d);

    const countAxisG = sel.selectAll("#countaxis")
      .attr("class", "axis")
      .data([props]);
    countAxisG.enter()
      .append("g")
      .attr("id", "countaxis")
      .attr("transform", "translate(" + left + ", 13)");
    countAxisG.call(axis);
  };
}

function errorsLegend(axis) {
  return function render(sel: d3.Selection<LegendProps>) {
    const { props } = sel.datum();

    sel.attr("class", "legend");

    const gradient = sel.selectAll("defs")
      .data([props]);
    gradient.enter()
      .append("defs")
      .append("svg:linearGradient")
      .attr("id", "gradient")
      .attr("x1", "0%")
      .attr("y1", "100%")
      .attr("x2", "100%")
      .attr("y2", "100%")
      .attr("spreadMethod", "pad");

    const stops = sel.select("#gradient").selectAll("stop")
      .data(errorColors);
    stops.enter()
      .append("stop")
      .attr("offset", (d, idx) => { return idx * (100 / (errorColors.length - 1)) + "%"; })
      .attr("stop-color", d => d)
      .attr("stop-opacity", 1);

    const left = 60;
    const legendBG = sel.selectAll("rect")
      .data([props]);
    legendBG.enter()
      .append("rect")
      .attr("fill", "url(#gradient)")
      .attr("fill-opacity", 1)
      .attr("stroke", "none")
      .attr("x", left)
      .attr("height", "14")
      .attr("transform", "translate(0, 43)");
    legendBG.attr("width", props.width);

    const legendName = sel.selectAll("text")
      .data([props]);
    legendName.enter()
      .append("text")
      .attr("class", "name")
      .attr("transform", "translate(0, 50)")
      .text("Errors");

    const domain = [0, props.metrics.maxErrors];
    const scale = d3.scale.linear().range([0, props.width]).domain(domain);
    axis.scale(scale)
      .tickSize(2, 2)
      .ticks(11)
      .tickFormat(d => d);

    const countAxisG = sel.selectAll("#countaxis")
      .attr("class", "axis")
      .data([props]);
    countAxisG.enter()
      .append("g")
      .attr("id", "countaxis")
      .attr("transform", "translate(" + left + ", 43)");
    countAxisG.call(axis);
  };
}

export class Legend extends React.Component<LegendProps> {
  count: React.ComponentClass<LegendProps>;
  errors: React.ComponentClass<LegendProps>;

  constructor(props: LegendProps) {
    super(props);
    this.count = createChartComponent("g", countLegend(d3.svg.axis().orient("top")));
    this.errors = createChartComponent("g", errorsLegend(d3.svg.axis().orient("top")));
  }

  render() {
    // tslint:disable-next-line:variable-name
    const Count = this.count;
    // tslint:disable-next-line:variable-name
    const Errors = this.errors;

    return (
        <div id="legend">
          <svg width="100%" height="60" display="block" preserveAspectRatio="none">
            <Count props={this.props} />
            <Errors props={this.props} />
          </svg>
        </div>
    );
  }
}
