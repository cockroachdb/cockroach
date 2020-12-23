// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import d3 from "d3";
import React from "react";

import { NanoToMilli } from "src/util/convert";
import { MetricsDataComponentProps } from "src/views/shared/components/metricQuery";
import createChartComponent from "src/views/shared/util/d3-react";
import { BACKGROUND_BLUE, MAIN_BLUE } from "src/views/shared/colors";

interface SparklineConfig {
  width: number;
  height: number;
  backgroundColor: string;
  foregroundColor: string;
  formatCurrentValue: (value: number) => string;
}

interface Datapoint {
  timestamp: number;
  value: number;
}

interface SparklineChartProps {
  results: Datapoint[];
}

function sparklineChart(config: SparklineConfig) {
  const {
    width,
    height,
    backgroundColor,
    foregroundColor,
    formatCurrentValue,
  } = config;
  const margin = {
    left: 1,
    top: 1,
    right: 1,
    bottom: 1,
  };

  const xScale = d3.scale.linear().range([margin.left, width - margin.right]);
  const yScale = d3.scale.linear().range([height - margin.bottom, margin.top]);

  const drawPath = d3.svg
    .line<Datapoint>()
    .x((d: Datapoint) => xScale(d.timestamp))
    .y((d: Datapoint) => yScale(d.value));

  return function renderSparkline(sel: d3.Selection<SparklineChartProps>) {
    // TODO(couchand): unsingletonize this
    const { results } = sel.datum();

    xScale.domain(d3.extent(results, (d: Datapoint) => d.timestamp));
    yScale.domain(d3.extent(results, (d: Datapoint) => d.value));

    const bg = sel.selectAll("rect").data([null]);

    bg.enter()
      .append("rect")
      .attr("width", width)
      .attr("height", height)
      .attr("fill", backgroundColor)
      .attr("fill-opacity", 1)
      .attr("stroke", "none");

    const line = sel.selectAll("path").data([results]);

    line
      .enter()
      .append("path")
      .attr("fill", "none")
      .attr("stroke", foregroundColor);

    line.attr("d", drawPath);

    const lastDatapoint =
      results && results.length ? results[results.length - 1].value : 0;

    const text = sel.selectAll("text").data([lastDatapoint]);

    text
      .enter()
      .append("text")
      .attr("x", width + 13)
      .attr("y", height - margin.bottom)
      .attr("text-anchor", "left")
      .attr("fill", foregroundColor)
      .attr("font-family", "Lato-Bold, Lato")
      .attr("font-size", 12)
      .attr("font-weight", 700);

    text.text(formatCurrentValue);
  };
}

interface SparklineMetricsDataComponentProps {
  formatCurrentValue: (value: number) => string;
}

export class SparklineMetricsDataComponent extends React.Component<
  MetricsDataComponentProps & SparklineMetricsDataComponentProps
> {
  chart: React.ComponentClass<SparklineChartProps>;

  constructor(
    props: MetricsDataComponentProps & SparklineMetricsDataComponentProps,
  ) {
    super(props);

    this.chart = createChartComponent(
      "g",
      sparklineChart({
        width: 69,
        height: 10,
        backgroundColor: BACKGROUND_BLUE,
        foregroundColor: MAIN_BLUE,
        formatCurrentValue: this.props.formatCurrentValue,
      }),
    );
  }

  render() {
    const { data } = this.props;
    if (!data || !data.results || !data.results.length) {
      return null;
    }

    const timestamps: number[] = [];
    const resultsByTimestamp: { [timestamp: string]: Datapoint } = {};

    data.results.forEach(({ datapoints }) => {
      datapoints.forEach(({ timestamp_nanos, value }) => {
        const timestamp = NanoToMilli(timestamp_nanos.toNumber());

        if (timestamps.indexOf(timestamp) !== -1) {
          resultsByTimestamp[timestamp].value += value;
        } else {
          resultsByTimestamp[timestamp] = { timestamp, value };
          timestamps.push(timestamp);
        }
      });
    });

    const results = timestamps.map(
      (timestamp) => resultsByTimestamp[timestamp],
    );

    const Sparkline = this.chart;

    return <Sparkline results={results} />;
  }
}
