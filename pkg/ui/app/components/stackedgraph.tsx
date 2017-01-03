import * as React from "react";
import * as nvd3 from "nvd3";
import { createSelector } from "reselect";

import { findChildrenOfType } from "../util/find";
import {
  MetricsDataComponentProps, Axis, AxisProps, InitLineChart, ConfigureLineChart,
  GraphLineState, mouseEnter, mouseMove, mouseLeave,
} from "./graphs";
import { Metric, MetricProps } from "./metric";
import Visualization from "./visualization";

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
export class StackedAreaGraph extends React.Component<StackedAreaGraphProps, GraphLineState> {
  // The SVG Element in the DOM used to render the graph.
  svgEl: SVGElement;

  state = new GraphLineState();

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
    InitLineChart(this.chart);

    this.chart.showControls(false);

    let range = axis.props.range;
    if (range) {
      if (range.length !== 2) {
        throw new Error("Unexpected range: " + range + ". " +
                        "For a stacked area chart, the range must be an array of length 2.");
      }
      this.chart.yDomain(range);
    }
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

      ConfigureLineChart(this.chart, this.svgEl, metrics, axis, this.props.data, this.props.timeInfo, true);
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
    let graphLineClass = "graph-lines__line";
    if (this.state.mouseIn) {
      graphLineClass += " graph-lines__line--hidden";
    }

    let mouseEnterBound = () => mouseEnter(this);
    let mouseLeaveBound = () => mouseLeave(this);

    return <Visualization title={title} subtitle={subtitle} tooltip={tooltip} loading={!data} >
      <div className="linegraph">
        <div className={graphLineClass}></div>
        <svg className="graph" ref={(svg) => this.svgEl = svg} onMouseMove={mouseMove} onMouseEnter={mouseEnterBound} onMouseLeave={mouseLeaveBound} />
      </div>
    </Visualization>;
  }
}
