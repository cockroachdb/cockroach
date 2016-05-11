import * as React from "react";
import * as nvd3 from "nvd3";
import * as d3 from "d3";
import { createSelector } from "reselect";
import _ = require("lodash");

import { findChildrenOfType } from "../util/find";
import { NanoToMilli } from "../util/convert";
import { MetricsDataComponentProps, Axis, AxisProps, Metric, MetricProps } from "./graphs";

// Chart margins to match design.
const CHART_MARGINS: nvd3.Margin = {top: 20, right: 60, bottom: 20, left: 60};

// Maximum number of series we will show in the legend. If there are more we hide the legend.
const MAX_LEGEND_SERIES: number = 3;

interface LineGraphProps extends MetricsDataComponentProps {
  title?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: string;
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
      .x((d: cockroach.ts.TimeSeriesDatapoint) => new Date(NanoToMilli(d && d.timestamp_nanos.toNumber())))
      .y((d: cockroach.ts.TimeSeriesDatapoint) => d && d.value)
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
      // Iterate through each selector on the axis,
      // allowing each to select the necessary data from
      // the result.

      // AxisDomain is a helper class used for storing the min/max values to store on an axis
      class AxisDomain {
        min: number;
        max: number;
        constructor(min: number = Infinity, max: number = -Infinity) {
          this.min = _.isNumber(min) ? min : this.min;
          this.max = _.isNumber(max) ? max : this.max;
        }

        domain(): [number, number] {
          return [this.min, this.max];
        }

        ticks(transform: (n: number) => any = _.identity): number[] {
          return _.map(_.uniq([this.min, (this.min + this.max) / 2, this.max]), transform);
        }
      }

      let yAxisDomain: AxisDomain = new AxisDomain();
      let xAxisDomain: AxisDomain = new AxisDomain();

      let computeFullAxisDomain = (domain: AxisDomain, values: number[]): AxisDomain => {
        return new AxisDomain(
          Math.min(domain.min, ...values),
          Math.max(domain.max, ...values)
        );
      };

      _.each(metrics, (s, idx) => {
        let result = this.props.data.results[idx];
        if (result) {
          yAxisDomain = computeFullAxisDomain(yAxisDomain, _.map(result.datapoints, (dp) => dp.value));
          xAxisDomain = computeFullAxisDomain(xAxisDomain, _.map(result.datapoints, (dp) => dp.timestamp_nanos.toNumber()));

          formattedData.push({
            values: result.datapoints || [],
            key: s.props.title || s.props.name,
            color: LineGraph.colors(s.props.name),
            area: true,
            fillOpacity: .1,
          });
        }
      });

      // compute final y axis display range using yDomain and ylow/yhigh values on the axis
      yAxisDomain = new AxisDomain(
        _.isNumber(axis.props.yLow) ? Math.min(yAxisDomain.min, axis.props.yLow) : yAxisDomain.min,
        _.isNumber(axis.props.yHigh) ? Math.max(yAxisDomain.max, axis.props.yHigh) : yAxisDomain.max
      );
      this.chart.yDomain(yAxisDomain.domain());

      // always set the tick values to the lowest axis value, the highest axis value, and one value in between
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
    return <div className="visualization-wrapper">
      <div className="viz-top"></div>
        <div className="linegraph">
          <svg className="graph" ref={(svg) => this.svgEl = svg}/>
        </div>
      <div className="viz-bottom">
        <div className="viz-title">{this.props.title}</div>
      </div>
    </div>;
  }
}

export { Axis, Metric } from "./graphs";
