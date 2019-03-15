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
import { errorColors } from "./legend";
import "./component_activity.styl";

interface ComponentActivityMetricsI {
  minSpanCount: number;
  maxSpanCount: number;
  minErrors: number;
  maxErrors: number;
  fillColor: (ca: protos.cockroach.util.tracing.IComponentActivity) => string;
  fillOpacity: (ca: protos.cockroach.util.tracing.IComponentActivity) => number;
}

export class ComponentActivityMetrics {
  count: number;
  minSpanCount: number;
  maxSpanCount: number;
  minErrors: number;
  maxErrors: number;

  constructor(){
    this.count = 0;
  }

  addComponentActivity(ca: protos.cockroach.util.tracing.IComponentActivity) {
    if (!ca) {
      return;
    }
    if (this.count == 0) {
      this.minSpanCount = ca.span_count;
      this.maxSpanCount = ca.span_count;
      this.minErrors = ca.errors;
      this.maxErrors = ca.errors;
    } else {
      if (ca.span_count.gt(this.maxSpanCount)) {
        this.maxSpanCount = ca.span_count;
      }
      if (ca.span_count.lt(this.minSpanCount)) {
        this.minSpanCount = ca.span_count;
      }
      if (ca.errors.gt(this.maxErrors)) {
        this.maxErrors = ca.errors;
      }
      if (ca.errors.lt(this.minErrors)) {
        this.minErrors = ca.errors;
      }
    }
    this.count++;
    if (this.minSpanCount.add(1).gt(this.maxSpanCount)) {
      this.maxSpanCount = this.minSpanCount.add(1);
    }
    if (this.minErrors.add(1).gt(this.maxErrors)) {
      this.maxErrors = this.minErrors.add(1);
    }
  }

  initScales() {
    if (this.countScale) {
      return;
    }
    this.countScale = d3.scale.linear().domain([0, this.maxSpanCount.toNumber()]).range([0.1, 1]);
    const step = d3.scale.linear().domain([1, errorColors.length]).range([0, this.maxErrors.toNumber()]);
    this.errorsScale = d3.scale.linear().domain([step(1), step(2), step(3), step(4), step(5), step(6), step(7)])
      .interpolate(d3.interpolateHcl)
      .range(errorColors);
  }

  fillColor(ca: protos.cockroach.util.tracing.IComponentActivity) {
    this.initScales();
    return this.errorsScale(ca.errors.toNumber());
  }
  fillOpacity(ca: protos.cockroach.util.tracing.IComponentActivity) {
    this.initScales();
    return this.countScale(ca.span_count.toNumber());
  }
}

interface ComponentActivityProps {
  metrics: ComponentActivityMetricsI;
  activity: protos.cockroach.util.tracing.IComponentActivity;
}

function activity() {
  return function render(sel: d3.Selection<ComponentActivityProps>) {
    const { metrics, activity, error } = sel.datum().props;

    const bg = sel.selectAll("rect")
      .data([activity]);

    bg.enter()
      .append("rect")
      .attr("stroke", "none")
      .attr("width", "100%")
      .attr("height", "100%");

    bg.attr("fill", error ? "#eee" : metrics.fillColor(activity))
      .attr("fill-opacity", error ? 1 : metrics.fillOpacity(activity));

    // Add optional stuck symbol.
    const stuckCount: number = activity.stuck_count.toNumber();
    if (stuckCount > 0) {
      const stuckG = sel.selectAll(".stuck-symbol")
        .data([stuckCount])
        .enter()
        .append("g")
        .attr("class", "stuck-symbol");
      stuckG.selectAll("path")
        .data([stuckCount])
        .enter()
        .append("path")
        .attr("transform", "scale(1.7)translate(7,6)")
        .attr("d", d3.svg.symbol().type("triangle-up"));
      stuckG.selectAll("text")
        .data([stuckCount])
        .enter()
        .append("text")
        .attr("transform", "translate(9.5,17)")
        .text("!");
    }
  };
}

export class ComponentActivity extends React.Component<ComponentActivityProps> {
  chart: React.ComponentClass<ComponentActivityProps>;

  constructor(props: ComponentActivityProps) {
    super(props);
    this.chart = createChartComponent("g", activity());
  }

  render() {
    // tslint:disable-next-line:variable-name
    const Chart = this.chart;
    return (
        <svg width="100%" height="20" display="block" preserveAspectRatio="none">
          <Chart props={this.props} />
        </svg>
    );
  }
}
