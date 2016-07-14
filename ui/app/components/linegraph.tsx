import * as React from "react";
import * as nvd3 from "nvd3";
import * as d3 from "d3";
import { createSelector } from "reselect";
import _ = require("lodash");

import { findChildrenOfType } from "../util/find";
import { NanoToMilli } from "../util/convert";
import {
  MetricsDataComponentProps, Axis, AxisProps, Metric, MetricProps, ProcessDataPoints,
} from "./graphs";
import Visualization from "./visualization";

// Chart margins to match design.
const CHART_MARGINS: nvd3.Margin = {top: 20, right: 60, bottom: 20, left: 60};

// Maximum number of series we will show in the legend. If there are more we hide the legend.
const MAX_LEGEND_SERIES: number = 3;

interface LineGraphProps extends MetricsDataComponentProps {
  title?: string;
  subtitle?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: React.ReactNode;
}

/**
 * LineGraph displays queried metrics in a line graph. It currently only
 * supports a single Y-axis, but multiple metrics can be graphed on the same
 * axis.
 */
export class LineGraph extends React.Component<LineGraphProps, {}> {
  static colors: d3.scale.Ordinal<string, string> = d3.scale.category10();

  // The SVG Element in the DOM used to render the graph.
  svgEl: SVGElement;

  // A configured NVD3 chart used to render the chart.
  chart: nvd3.LineChart;

  axis = createSelector(
    (props: {children?: any}) => props.children,
    (children) => {
      let axes: React.ReactElement<AxisProps>[] = findChildrenOfType(children, Axis);
      if (axes.length === 0) {
        console.warn("LineGraph requires the specification of at least one axis.");
        return null;
      }
      if (axes.length > 1) {
        console.warn("LineGraph currently only supports a single axis; ignoring additional axes.");
      }
      return axes[0];
    });

  metrics = createSelector(
    (props: {children?: any}) => props.children,
    (children) => {
      return findChildrenOfType(children, Metric) as React.ReactElement<MetricProps>[];
    });

  initChart() {
    let axis = this.axis(this.props);
    if (!axis) {
      // TODO: Figure out this error condition.
      return;
    }

    this.chart = nvd3.models.lineChart();
    this.chart
      .x((d: cockroach.ts.tspb.TimeSeriesDatapoint) => new Date(NanoToMilli(d && d.timestamp_nanos.toNumber())))
      .y((d: cockroach.ts.tspb.TimeSeriesDatapoint) => d && d.value)
      .useInteractiveGuideline(true)
      .showLegend(true)
      .showYAxis(true)
      .showXAxis(this.props.xAxis || true)
      .xScale(d3.time.scale())
      .margin(CHART_MARGINS);

    this.chart.xAxis
      .tickFormat((t) => typeof t === "string" ? t : d3.time.format("%H:%M:%S")(t))
      .showMaxMin(false);
    this.chart.yAxis
      .axisLabel(axis.props.label)
      .showMaxMin(false);

    if (axis.props.format) {
      this.chart.yAxis.tickFormat(axis.props.format);
    }

    if (axis.props.range) {
      this.chart.forceY(axis.props.range);
    }
  }

  drawChart() {
    let metrics = this.metrics(this.props);
    let axis = this.axis(this.props);
    if (!axis) {
      return;
    }

    this.chart.showLegend(_.isBoolean(this.props.legend) ? this.props.legend :
      metrics.length > 1 && metrics.length <= MAX_LEGEND_SERIES);
    let formattedData: any[] = [];

    if (this.props.data)  {
      let processed = ProcessDataPoints(metrics, axis, this.props.data);
      formattedData = processed.formattedData;
      let {yAxisDomain, xAxisDomain } = processed;

      this.chart.yDomain(yAxisDomain.domain());

      // always set the tick values to the lowest axis value, the highest axis
      // value, and one value in between
      this.chart.yAxis.tickValues(yAxisDomain.ticks());
      this.chart.xAxis.tickValues(xAxisDomain.ticks((n) => new Date(NanoToMilli(n))));
    }

    d3.select(this.svgEl)
      .datum(formattedData)
      .transition().duration(500)
      .call(this.chart);
  }

  componentDidMount() {
    this.initChart();
    this.drawChart();
  }

  componentDidUpdate() {
    this.drawChart();
  }

  render() {
    let { title, subtitle, tooltip } = this.props;
    return <Visualization title={title} subtitle={subtitle} tooltip={tooltip}>
      <div className="linegraph">
        <svg className="graph" ref={(svg) => this.svgEl = svg}/>
      </div>
    </Visualization>;
  }
}

export { Axis, Metric } from "./graphs";
