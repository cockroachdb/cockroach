import * as React from "react";
import * as nvd3 from "nvd3";
import * as d3 from "d3";
import { createSelector } from "reselect";
import _ from "lodash";

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

interface StackedAreaGraphProps extends MetricsDataComponentProps {
  title?: string;
  subtitle?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: string;
}

/**
 * StackedAreaGraph displays queried metrics in a stacked area graph. It
 * currently only supports a single Y-axis, but multiple metrics can be graphed
 * on the same axis.
 */
export class StackedAreaGraph extends React.Component<StackedAreaGraphProps, {}> {
  // The SVG Element in the DOM used to render the graph.
  svgEl: SVGElement;

  // A configured NVD3 chart used to render the chart.
  chart: nvd3.StackedAreaChart;

  axis = createSelector(
    (props: {children?: any}) => props.children,
    (children) => {
      let axes: React.ReactElement<AxisProps>[] = findChildrenOfType(children, Axis);
      if (axes.length === 0) {
        console.warn("StackedAreaGraph requires the specification of at least one axis.");
        return null;
      }
      if (axes.length > 1) {
        console.warn("StackedAreaGraph currently only supports a single axis; ignoring additional axes.");
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

    this.chart = nvd3.models.stackedAreaChart();
    this.chart
      .x((d: Proto2TypeScript.cockroach.ts.tspb.TimeSeriesDatapoint) => new Date(NanoToMilli(d && d.timestamp_nanos.toNumber())))
      .y((d: Proto2TypeScript.cockroach.ts.tspb.TimeSeriesDatapoint) => d && d.value)
      .useInteractiveGuideline(true)
      .showLegend(true)
      .showYAxis(true)
      .showXAxis(this.props.xAxis || true)
      .xScale(d3.time.scale())
      .margin(CHART_MARGINS);

    this.chart.showControls(false);

    this.chart.xAxis
      .tickFormat((t) => typeof t === "string" ? t : d3.time.format("%H:%M:%S")(t))
      .showMaxMin(false);
    this.chart.yAxis
      .axisLabel(axis.props.label)
      .showMaxMin(false);

    if (axis.props.format) {
      this.chart.yAxis.tickFormat(axis.props.format);
    }

    let range = axis.props.range;
    if (range) {
      if (range.length !== 2) {
        throw new Error("Unexpected range: " + range + ". " +
                        "For a stacked area chart, the range must be an array of length 2.");
      }
      this.chart.yDomain(range);
    }
  }

  drawChart() {
    // If the document is not visible (e.g. if the window is minimized) we don't
    // attempt to redraw the chart. Redrawing the chart uses
    // requestAnimationFrame, which isn't called when the tab is in the
    // background, and is then apparently queued up and called en masse when the
    // tab re-enters the foreground. This check prevents the issue in #8896
    // where switching to a tab with the graphs page open that had been in the
    // background caused the UI to run out of memory and either lag or crash.
    // NOTE: This might not work on Android:
    // http://caniuse.com/#feat=pagevisibility
    if (!document.hidden) {
      let metrics = this.metrics(this.props);
      let axis = this.axis(this.props);
      if (!axis) {
        return;
      }

      this.chart.showLegend(_.isBoolean(this.props.legend) ? this.props.legend :
        metrics.length > 1 && metrics.length <= MAX_LEGEND_SERIES);
      let formattedData: any[] = [];

      if (this.props.data) {
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
  }

  componentDidMount() {
    this.initChart();
    this.drawChart();
    // NOTE: This might not work on Android:
    // http://caniuse.com/#feat=pagevisibility
    // TODO (maxlang): Check if this element is visible based on scroll state.
    document.addEventListener("visibilitychange", this.drawChart);
  }

  componentWillUnmount() {
    document.removeEventListener("visibilitychange", this.drawChart);
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
