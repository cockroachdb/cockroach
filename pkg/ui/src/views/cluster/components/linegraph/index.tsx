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
import moment from "moment";
import * as nvd3 from "nvd3";
import { createSelector } from "reselect";

import * as protos from  "src/js/protos";
import { HoverState, hoverOn, hoverOff } from "src/redux/hover";
import { findChildrenOfType } from "src/util/find";
import {
  ConfigureLineChart, InitLineChart, CHART_MARGINS, ConfigureLinkedGuideline,
} from "src/views/cluster/util/graphs";
import {
  Metric, MetricProps, Axis, AxisProps, QueryTimeInfo,
} from "src/views/shared/components/metricQuery";
import { MetricsDataComponentProps } from "src/views/shared/components/metricQuery";
import Visualization from "src/views/cluster/components/visualization";
import { NanoToMilli } from "src/util/convert";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

interface LineGraphProps extends MetricsDataComponentProps {
  title?: string;
  subtitle?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: React.ReactNode;
  hoverOn?: typeof hoverOn;
  hoverOff?: typeof hoverOff;
  hoverState?: HoverState;
}

interface LineGraphState {
  lastData?: TSResponse;
  lastTimeInfo?: QueryTimeInfo;
}

/**
 * LineGraph displays queried metrics in a line graph. It currently only
 * supports a single Y-axis, but multiple metrics can be graphed on the same
 * axis.
 */
export class LineGraph extends React.Component<LineGraphProps, LineGraphState> {
  // The SVG Element reference in the DOM used to render the graph.
  graphEl: React.RefObject<SVGSVGElement> = React.createRef();

  // A configured NVD3 chart used to render the chart.
  chart: nvd3.LineChart;

  axis = createSelector(
    (props: {children?: React.ReactNode}) => props.children,
    (children) => {
      const axes: React.ReactElement<AxisProps>[] = findChildrenOfType(children as any, Axis);
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
      return findChildrenOfType(children as any, Metric) as React.ReactElement<MetricProps>[];
    });

  initChart() {
    const axis = this.axis(this.props);
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

  mouseMove = (e: any) => {
    // TODO(couchand): handle the following cases:
    //   - first series is missing data points
    //   - series are missing data points at different timestamps
    const datapoints = this.props.data.results[0].datapoints;
    const timeScale = this.chart.xAxis.scale();

    // To get the x-coordinate within the chart we subtract the left side of the SVG
    // element and the left side margin.
    const x = e.clientX - this.graphEl.current.getBoundingClientRect().left - CHART_MARGINS.left;
    // Find the time value of the coordinate by asking the scale to invert the value.
    const t = Math.floor(timeScale.invert(x));

    // Find which data point is closest to the x-coordinate.
    let result: moment.Moment;
    if (datapoints.length) {
      const series: any = datapoints.map((d: any) => NanoToMilli(d.timestamp_nanos.toNumber()));

      const right = d3.bisectRight(series, t);
      const left = right - 1;

      let index = 0;

      if (right >= series.length) {
        // We're hovering over the rightmost point.
        index = left;
      } else if (left < 0) {
        // We're hovering over the leftmost point.
        index = right;
      } else {
        // The general case: we're hovering somewhere over the middle.
        const leftDistance = t - series[left];
        const rightDistance = series[right] - t;

        index = leftDistance < rightDistance ? left : right;
      }

      result = moment(new Date(series[index]));
    }

    if (!this.props.hoverState || !result) {
      return;
    }

    // Only dispatch if we have something to change to avoid action spamming.
    if (this.props.hoverState.hoverChart !== this.props.title || !result.isSame(this.props.hoverState.hoverTime)) {
      this.props.hoverOn({
        hoverChart: this.props.title,
        hoverTime: result,
      });
    }
  }

  mouseLeave = () => {
    this.props.hoverOff();
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
      const metrics = this.metrics(this.props);
      const axis = this.axis(this.props);
      if (!axis) {
        return;
      }

      ConfigureLineChart(
        this.chart, this.graphEl.current, metrics, axis, this.props.data, this.props.timeInfo,
      );
    }
  }

  drawLine = () => {
    if (!document.hidden) {
      let hoverTime: moment.Moment;
      if (this.props.hoverState) {
        const { currentlyHovering, hoverChart } = this.props.hoverState;
        // Don't draw the linked guideline on the hovered chart, NVD3 does that for us.
        if (currentlyHovering && hoverChart !== this.props.title) {
          hoverTime = this.props.hoverState.hoverTime;
        }
      }

      const axis = this.axis(this.props);
      ConfigureLinkedGuideline(this.chart, this.graphEl.current, axis, this.props.data, hoverTime);
    }
  }

  constructor(props: any) {
    super(props);
    this.state = {
      lastData: null,
      lastTimeInfo: null,
    };
  }

  componentDidMount() {
    this.initChart();
    this.drawChart();
    this.drawLine();
    // NOTE: This might not work on Android:
    // http://caniuse.com/#feat=pagevisibility
    // TODO (maxlang): Check if this element is visible based on scroll state.
    document.addEventListener("visibilitychange", this.drawChart);
  }

  componentWillUnmount() {
    document.removeEventListener("visibilitychange", this.drawChart);
  }

  componentDidUpdate() {
    if (this.props.data !== this.state.lastData || this.props.timeInfo !== this.state.lastTimeInfo) {
      this.drawChart();
      this.setState({
        lastData: this.props.data,
        lastTimeInfo: this.props.timeInfo,
      });
    }
    this.drawLine();
  }

  render() {
    const { title, subtitle, tooltip, data } = this.props;

    let hoverProps: Partial<React.SVGProps<SVGSVGElement>> = {};
    if (this.props.hoverOn) {
      hoverProps = {
        onMouseMove: this.mouseMove,
        onMouseLeave: this.mouseLeave,
      };
    }

    return (
      <Visualization title={title} subtitle={subtitle} tooltip={tooltip} loading={!data} >
        <div className="linegraph">
          <svg className="graph linked-guideline" ref={this.graphEl} {...hoverProps} />
        </div>
      </Visualization>
    );
  }
}
