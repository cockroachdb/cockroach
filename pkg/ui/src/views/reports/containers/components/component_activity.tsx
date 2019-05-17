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
import * as time_util from "./time_util";

import "./component_activity.styl";

export class ComponentActivityRates {
  span_rate: number;
  event_rate: number;
  error_rate: number;
  stuck_count: number;

  constructor(ca?: protos.cockroach.util.tracing.IComponentActivity,
              last?: protos.cockroach.util.tracing.IComponentActivity,
              server_ts?: protos.google.protobuf.Timestamp) {
    if (ca && last) {
      const diff: number = time_util.durationAsNumber(time_util.subtractTimestamps(ca.timestamp, last.timestamp));
      this.span_rate = ca.span_count.sub(last.span_count).toNumber() / diff;
      this.event_rate = ca.event_count.sub(last.event_count).toNumber() / diff;
      this.error_rate = ca.errors.sub(last.errors).toNumber() / diff;
      this.stuck_count = ca.stuck_count.toNumber();
    } else if (ca && server_ts) {
      const diff: number = time_util.durationAsNumber(time_util.subtractTimestamps(ca.timestamp, server_ts));
      this.span_rate = ca.span_count.toNumber() / diff;
      this.event_rate = ca.event_count.toNumber() / diff;
      this.error_rate = ca.errors.toNumber() / diff;
      this.stuck_count = ca.stuck_count.toNumber();
    } else {
      this.span_rate = 0;
      this.event_rate = 0;
      this.error_rate = 0;
      this.stuck_count = 0;
    }
  }
}

interface IComponentActivityMetrics {
  min_span_rate: number;
  max_span_rate: number;
  min_error_rate: number;
  max_error_rate: number;
  fillColor: (ca: ComponentActivityRates) => string;
  fillOpacity: (ca: ComponentActivityRates) => number;
}

export class ComponentActivityMetrics {
  count: number;
  min_span_rate: number;
  max_span_rate: number;
  min_error_rate: number;
  max_error_rate: number;

  constructor(){
    this.count = 0;
  }

  addComponentActivity(ca: ComponentActivityRates) {
    if (!ca) {
      return;
    }
    if (this.count == 0) {
      this.min_span_rate = ca.span_rate;
      this.max_span_rate = ca.span_rate;
      this.min_error_rate = ca.error_rate;
      this.max_error_rate = ca.error_rate;
    } else {
      if (ca.span_rate > this.max_span_rate) {
        this.max_span_rate = ca.span_rate;
      }
      if (ca.span_rate < this.min_span_rate) {
        this.min_span_rate = ca.span_rate;
      }
      if (ca.error_rate > this.max_error_rate) {
        this.max_error_rate = ca.error_rate;
      }
      if (ca.error_rate < this.min_error_rate) {
        this.min_error_rate = ca.error_rate;
      }
    }
    this.count++;
    if (this.min_span_rate + 1 > this.max_span_rate) {
      this.max_span_rate = this.min_span_rate + 1;
    }
    if (this.min_error_rate + 1 > this.max_error_rate) {
      this.max_error_rate = this.min_error_rate + 1;
    }
  }

  initScales() {
    if (this.countScale) {
      return;
    }
    this.countScale = d3.scale.linear().domain([0, this.max_span_rate]).range([0.1, 1]);
    const step = d3.scale.linear().domain([1, errorColors.length]).range([0, this.max_error_rate]);
    this.errorsScale = d3.scale.linear().domain([step(1), step(2), step(3), step(4), step(5), step(6), step(7)])
      .interpolate(d3.interpolateHcl)
      .range(errorColors);
  }

  fillColor(ca: ComponentActivityRates) {
    this.initScales();
    return this.errorsScale(ca.error_rate);
  }
  fillOpacity(ca: ComponentActivityRates) {
    this.initScales();
    return this.countScale(ca.span_rate);
  }
}

interface ComponentActivityProps {
  metrics: IComponentActivityMetrics;
  activity: ComponentActivityRates;
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
    const stuck_data: number[] = [];
    if (activity.stuck_count > 0) {
      stuck_data.push(activity.stuck_count);
    }
    const stuckSel = sel.selectAll(".stuck-symbol").data(stuck_data);
    const stuckG = stuckSel.enter().append("g")
      .attr("class", "stuck-symbol");
    stuckSel.exit().remove();
    stuckG.append("path")
      .attr("transform", "scale(1.7)translate(7,6)")
      .attr("d", d3.svg.symbol().type("triangle-up"));
    stuckG.append("text")
      .attr("transform", "translate(9.5,17)")
      .text("!");
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
