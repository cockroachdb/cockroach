// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import d3 from "d3";
import React from "react";

import { Bytes, Percentage } from "src/util/format";

interface BytesBarChartProps {
  used: number;
  usable: number;
}

const width = 175;
const height = 28;

const chartHeight = 14;

export class BytesBarChart extends React.Component<BytesBarChartProps> {
  chart: React.RefObject<HTMLDivElement> = React.createRef();

  componentDidMount() {
    this.renderChart(this.props);
  }

  shouldComponentUpdate(props: BytesBarChartProps) {
    this.renderChart(props);
    return false;
  }

  renderChart(props: BytesBarChartProps) {
    const svg = d3
      .select(this.chart.current)
      .selectAll("svg")
      .data([{ width, height }]);

    const svgEnter = svg
      .enter()
      .append("svg")
      .attr("width", (d) => d.width)
      .attr("height", (d) => d.height);

    svgEnter
      .append("text")
      .attr("y", chartHeight - 4)
      .attr("class", "bar-chart__label percent");

    const label = svg.select(".bar-chart__label.percent").text("100%");

    const textNode = label.node();
    const reserveWidth = !textNode
      ? 100
      : (textNode as SVGSVGElement).getBBox().width;

    svg
      .selectAll(".bar-chart__label.percent")
      .text(Percentage(props.used, props.usable));

    const labelWidth = !textNode
      ? 100
      : (textNode as SVGSVGElement).getBBox().width;

    label.attr("x", reserveWidth - labelWidth);

    const spacing = 3;

    const chartEnter = svgEnter
      .append("g")
      .attr("class", "bar-chart__bars")
      .attr("transform", `translate(${reserveWidth + spacing},0)`);

    const chart = svg.selectAll(".bar-chart__bars");

    const chartWidth = width - reserveWidth - spacing;

    chartEnter
      .append("rect")
      .attr("width", chartWidth * 0.9)
      .attr("height", chartHeight)
      .attr("fill", "#e2e5ee");

    chartEnter
      .append("rect")
      .attr("x", chartWidth * 0.9)
      .attr("width", chartWidth * 0.1)
      .attr("height", chartHeight)
      .attr("fill", "#cfd2dc");

    chartEnter
      .append("rect")
      .attr("class", "bar-chart__bar used")
      .attr("height", 5) // This attr's for FF, the CSS rule is for Chrome.
      .attr("y", (chartHeight - 5) / 2);

    chart
      .selectAll(".bar-chart__bar.used")
      .attr("width", (chartWidth * props.used) / props.usable);

    chartEnter
      .append("text")
      .attr("y", height)
      .attr("class", "bar-chart__label used");

    chart.selectAll(".bar-chart__label.used").text(Bytes(props.used));

    chartEnter
      .append("text")
      .attr("y", height)
      .attr("class", "bar-chart__label total");

    const total = chart
      .selectAll(".bar-chart__label.total")
      .text(Bytes(props.usable));

    const totalNode = total.node();
    const totalWidth = !totalNode
      ? 100
      : (totalNode as SVGSVGElement).getBBox().width;

    total.attr("x", chartWidth - totalWidth - 0);
  }

  render() {
    return <div ref={this.chart} />;
  }
}
