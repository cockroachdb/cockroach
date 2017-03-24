import * as React from "react";
import * as nvd3 from "nvd3";
import { createSelector } from "reselect";

import { findChildrenOfType } from "../util/find";
import {
  MetricsDataComponentProps, Axis, AxisProps, ConfigureLineChart, InitLineChart,
  updateLinkedGuidelines,
} from "./graphs";
import { Metric, MetricProps } from "./metric";
import Visualization from "./visualization";

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
  // The SVG Element in the DOM used to render the graph.
  graphEl: SVGElement;

  // A configured NVD3 chart used to render the chart.
  chart: nvd3.LineChart;

  axis = createSelector(
    (props: {children?: React.ReactNode}) => props.children,
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
    (props: {children?: React.ReactNode}) => props.children,
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
    InitLineChart(this.chart);

    if (axis.props.range) {
      this.chart.forceY(axis.props.range);
    }
  }

  mouseMove = () => {
    updateLinkedGuidelines(this.graphEl);
  }

  drawChart = () => {
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

      ConfigureLineChart(this.chart, this.graphEl, metrics, axis, this.props.data, this.props.timeInfo, false);
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
    let { title, subtitle, tooltip, data } = this.props;

    return <Visualization title={title} subtitle={subtitle} tooltip={tooltip} loading={!data} >
      <div className="linegraph">
        <svg className="graph linked-guideline" ref={(svg) => this.graphEl = svg} onMouseMove={this.mouseMove} onMouseLeave={this.mouseMove} />
      </div>
    </Visualization>;
  }
}
