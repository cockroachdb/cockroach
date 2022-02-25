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

import * as protos from "src/js/protos";
import { hoverOff, hoverOn, HoverState } from "src/redux/hover";
import { findChildrenOfType } from "src/util/find";
import {
  AxisDomain,
  calculateXAxisDomain,
  calculateYAxisDomain,
  CHART_MARGINS,
  ConfigureLineChart,
  ConfigureLinkedGuideline,
  configureUPlotLineChart,
  formatMetricData,
  formattedSeries,
  InitLineChart,
} from "src/views/cluster/util/graphs";
import {
  Axis,
  AxisProps,
  Metric,
  MetricProps,
  MetricsDataComponentProps,
  QueryTimeInfo,
} from "src/views/shared/components/metricQuery";
import Visualization from "src/views/cluster/components/visualization";
import { TimeScale, util } from "@cockroachlabs/cluster-ui";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import Long from "long";
import {
  findClosestTimeScale,
  defaultTimeScaleOptions,
  TimeWindow,
} from "@cockroachlabs/cluster-ui";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

export interface LineGraphProps extends MetricsDataComponentProps {
  title?: string;
  subtitle?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: React.ReactNode;
  hoverOn?: typeof hoverOn;
  hoverOff?: typeof hoverOff;
  hoverState?: HoverState;
  preCalcGraphSize?: boolean;
}

interface LineGraphStateOld {
  lastData?: TSResponse;
  lastTimeInfo?: QueryTimeInfo;
}

/**
 * LineGraph displays queried metrics in a line graph. It currently only
 * supports a single Y-axis, but multiple metrics can be graphed on the same
 * axis.
 */
export class LineGraphOld extends React.Component<
  LineGraphProps,
  LineGraphStateOld
> {
  // The SVG Element reference in the DOM used to render the graph.
  graphEl: React.RefObject<SVGSVGElement> = React.createRef();

  // A configured NVD3 chart used to render the chart.
  chart: nvd3.LineChart;

  axis = createSelector(
    (props: { children?: React.ReactNode }) => props.children,
    children => {
      const axes: React.ReactElement<AxisProps>[] = findChildrenOfType(
        children as any,
        Axis,
      );
      if (axes.length === 0) {
        console.warn(
          "LineGraph requires the specification of at least one axis.",
        );
        return null;
      }
      if (axes.length > 1) {
        console.warn(
          "LineGraph currently only supports a single axis; ignoring additional axes.",
        );
      }
      return axes[0];
    },
  );

  metrics = createSelector(
    (props: { children?: React.ReactNode }) => props.children,
    children => {
      return findChildrenOfType(children as any, Metric) as React.ReactElement<
        MetricProps
      >[];
    },
  );

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
    const x =
      e.clientX -
      this.graphEl.current.getBoundingClientRect().left -
      CHART_MARGINS.left;
    // Find the time value of the coordinate by asking the scale to invert the value.
    const t = Math.floor(timeScale.invert(x));

    // Find which data point is closest to the x-coordinate.
    let result: moment.Moment;
    if (datapoints.length) {
      const series: any = datapoints.map((d: any) =>
        util.NanoToMilli(d.timestamp_nanos.toNumber()),
      );

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
    if (
      this.props.hoverState.hoverChart !== this.props.title ||
      !result.isSame(this.props.hoverState.hoverTime)
    ) {
      this.props.hoverOn({
        hoverChart: this.props.title,
        hoverTime: result,
      });
    }
  };

  mouseLeave = () => {
    this.props.hoverOff();
  };

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
        this.chart,
        this.graphEl.current,
        metrics,
        axis,
        this.props.data,
        this.props.timeInfo,
      );
    }
  };

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
      ConfigureLinkedGuideline(
        this.chart,
        this.graphEl.current,
        axis,
        this.props.data,
        hoverTime,
      );
    }
  };

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
    if (
      this.props.data !== this.state.lastData ||
      this.props.timeInfo !== this.state.lastTimeInfo
    ) {
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
      <Visualization
        title={title}
        subtitle={subtitle}
        tooltip={tooltip}
        loading={!data}
      >
        <div className="linegraph">
          <svg
            className="graph linked-guideline"
            ref={this.graphEl}
            {...hoverProps}
          />
        </div>
      </Visualization>
    );
  }
}

// touPlot formats our timeseries data into the format
// uPlot expects which is a 2-dimensional array where the
// first array contains the x-values (time).
function touPlot(
  data: formattedSeries[],
  sampleDuration?: Long,
): uPlot.AlignedData {
  // Here's an example of what this code is attempting to control for.
  // We produce `result` series that contain their own x-values. uPlot
  // expects *one* x-series that all y-values match up to. So first we
  // need to take the union of all timestamps across all series, and then
  // produce y-values that match up to those. Any missing values will
  // be set to null and uPlot expects that.
  //
  // our data: [
  //   [(11:00, 1),             (11:05, 2),  (11:06, 3), (11:10, 4),           ],
  //   [(11:00, 1), (11:03, 20),             (11:06, 7),            (11:11, 40)],
  // ]
  //
  // for uPlot: [
  //   [11:00, 11:03, 11:05, 11:06, 11:10, 11:11]
  //   [1, null, 2, 3, 4, null],
  //   [1, 20, null, 7, null, 40],
  // ]
  if (!data || data.length === 0) {
    return [[]];
  }

  const xValuesComplete: number[] = [
    ...new Set(
      data.flatMap(series =>
        series.values.map(d => d.timestamp_nanos.toNumber()),
      ),
    ),
  ].sort((a, b) => a - b);

  const xValuesWithGaps = fillGaps(xValuesComplete, sampleDuration);

  const yValuesComplete: (number | null)[][] = data.map(series => {
    return xValuesWithGaps.map(ts => {
      const found = series.values.find(
        dp => dp.timestamp_nanos.toNumber() === ts,
      );
      return found ? found.value : null;
    });
  });

  return [xValuesWithGaps.map(ts => util.NanoToMilli(ts)), ...yValuesComplete];
}

// TODO (koorosh): the same logic can be achieved with uPlot's series.gaps API starting from 1.6.15 version.
export function fillGaps(
  data: uPlot.AlignedData[0],
  sampleDuration?: Long,
): uPlot.AlignedData[0] {
  if (data.length === 0 || !sampleDuration) {
    return data;
  }
  const sampleDurationMillis = sampleDuration.toNumber();
  const dataPointsNumber = data.length;
  const expectedPointsNumber =
    (data[data.length - 1] - data[0]) / sampleDurationMillis + 1;
  if (dataPointsNumber === expectedPointsNumber) {
    return data;
  }
  const yDataWithGaps: number[] = [];
  // validate time intervals for y axis data
  data.forEach((d, idx, arr) => {
    // case for the last item
    if (idx === arr.length - 1) {
      yDataWithGaps.push(d);
    }
    const nextItem = data[idx + 1];
    if (nextItem - d <= sampleDurationMillis) {
      yDataWithGaps.push(d);
      return;
    }
    for (
      let i = d;
      nextItem - i >= sampleDurationMillis;
      i = i + sampleDurationMillis
    ) {
      yDataWithGaps.push(i);
    }
  });
  return yDataWithGaps;
}

// LineGraph wraps the uPlot library into a React component
// when the component is first initialized, we wait until
// data is available and then construct the uPlot object
// and store its ref in a global variable.
// Once we receive updates to props, we push new data to the
// uPlot object.
export class LineGraph extends React.Component<LineGraphProps, {}> {
  constructor(props: LineGraphProps) {
    super(props);

    this.setNewTimeRange = this.setNewTimeRange.bind(this);
  }

  // axis is copied from the nvd3 LineGraph component above
  axis = createSelector(
    (props: { children?: React.ReactNode }) => props.children,
    children => {
      const axes: React.ReactElement<AxisProps>[] = findChildrenOfType(
        children as any,
        Axis,
      );
      if (axes.length === 0) {
        console.warn(
          "LineGraph requires the specification of at least one axis.",
        );
        return null;
      }
      if (axes.length > 1) {
        console.warn(
          "LineGraph currently only supports a single axis; ignoring additional axes.",
        );
      }
      return axes[0];
    },
  );

  // metrics is copied from the nvd3 LineGraph component above
  metrics = createSelector(
    (props: { children?: React.ReactNode }) => props.children,
    children => {
      return findChildrenOfType(children as any, Metric) as React.ReactElement<
        MetricProps
      >[];
    },
  );

  // setNewTimeRange uses code from the TimeScaleDropdown component
  // to set new start/end ranges in the query params and force a
  // reload of the rest of the dashboard at new ranges via the props
  // `setMetricsFixedWindow` and `setTimeScale`.
  // TODO(davidh): centralize management of query params for time range
  // TODO(davidh): figure out why the timescale doesn't get more granular
  // automatically when a narrower time frame is selected.
  setNewTimeRange(startMillis: number, endMillis: number) {
    if (startMillis === endMillis) return;
    const start = util.MilliToSeconds(startMillis);
    const end = util.MilliToSeconds(endMillis);
    const newTimeWindow: TimeWindow = {
      start: moment.unix(start),
      end: moment.unix(end),
    };
    let newTimeScale: TimeScale = {
      ...findClosestTimeScale(defaultTimeScaleOptions, end - start, start),
      key: "Custom",
      fixedWindowEnd: moment.unix(end),
    };
    if (this.props.adjustTimeScaleOnChange) {
      newTimeScale = this.props.adjustTimeScaleOnChange(
        newTimeScale,
        newTimeWindow,
      );
    }
    this.props.setMetricsFixedWindow(newTimeWindow);
    this.props.setTimeScale(newTimeScale);
    const { pathname, search } = this.props.history.location;
    const urlParams = new URLSearchParams(search);

    urlParams.set("start", moment.unix(start).format("X"));
    urlParams.set("end", moment.unix(end).format("X"));

    this.props.history.push({
      pathname,
      search: urlParams.toString(),
    });
  }

  u: uPlot;
  el = React.createRef<HTMLDivElement>();

  // yAxisDomain holds our computed AxisDomain object
  // for the y Axis. The function to compute this was
  // created to support the prior iteration
  // of our line graphs. We recompute it manually
  // when data changes, and uPlot options have access
  // to a closure that holds a reference to this value.
  yAxisDomain: AxisDomain;

  // xAxisDomain holds our computed AxisDomain object
  // for the x Axis. The function to compute this was
  // created to support the prior iteration
  // of our line graphs. We recompute it manually
  // when data changes, and uPlot options have access
  // to a closure that holds a reference to this value.
  xAxisDomain: AxisDomain;

  componentDidUpdate(prevProps: Readonly<LineGraphProps>) {
    if (
      !this.props.data?.results ||
      (prevProps.data === this.props.data && this.u !== undefined)
    ) {
      return;
    }

    const data = this.props.data;
    const metrics = this.metrics(this.props);
    const axis = this.axis(this.props);

    const fData = formatMetricData(metrics, data);
    const uPlotData = touPlot(fData, this.props.timeInfo?.sampleDuration);

    // The values of `this.yAxisDomain` and `this.xAxisDomain`
    // are captured in arguments to `configureUPlotLineChart`
    // and are called when recomputing certain axis and
    // series options. This lets us use updated domains
    // when redrawing the uPlot chart on data change.
    this.yAxisDomain = calculateYAxisDomain(axis.props.units, data);
    this.xAxisDomain = calculateXAxisDomain(this.props.timeInfo);

    const prevKeys =
      prevProps.data && prevProps.data.results
        ? formatMetricData(metrics, prevProps.data).map(s => s.key)
        : [];
    const keys = fData.map(s => s.key);
    const sameKeys =
      keys.every(k => prevKeys.includes(k)) &&
      prevKeys.every(k => keys.includes(k));

    if (
      this.u && // we already created a uPlot instance
      prevProps.data && // prior update had data as well
      sameKeys // prior update had same set of series identified by key
    ) {
      // The axis label option on uPlot doesn't accept
      // a function that recomputes the label, so we need
      // to manually update it in cases where we change
      // the scale (this happens on byte/time-based Y
      // axes where can change from MiB or KiB scales,
      // for instance).
      this.u.axes[1].label =
        axis.props.label +
        (this.yAxisDomain.label ? ` (${this.yAxisDomain.label})` : "");

      // Updates existing plot with new points
      this.u.setData(uPlotData);
    } else {
      const options = configureUPlotLineChart(
        metrics,
        axis,
        data,
        this.setNewTimeRange,
        () => this.xAxisDomain,
        () => this.yAxisDomain,
      );

      if (this.u) {
        this.u.destroy();
      }
      this.u = new uPlot(options, uPlotData, this.el.current);
    }
  }

  componentWillUnmount() {
    if (this.u) {
      this.u.destroy();
      this.u = null;
    }
  }

  render() {
    const { title, subtitle, tooltip, data, preCalcGraphSize } = this.props;

    return (
      <Visualization
        title={title}
        subtitle={subtitle}
        tooltip={tooltip}
        loading={!data}
        preCalcGraphSize={preCalcGraphSize}
      >
        <div className="linegraph">
          <div ref={this.el} />
        </div>
      </Visualization>
    );
  }
}
