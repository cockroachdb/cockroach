// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  calculateXAxisDomain,
  calculateYAxisDomain,
  AxisDomain,
  TimeScale,
  Visualization,
  util,
  WithTimezoneProps,
  findClosestTimeScale,
  defaultTimeScaleOptions,
  TimeWindow,
  WithTimezone,
} from "@cockroachlabs/cluster-ui";
import { Tooltip } from "antd";
import filter from "lodash/filter";
import flatMap from "lodash/flatMap";
import Long from "long";
import moment from "moment-timezone";
import React from "react";
import { createSelector } from "reselect";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

import * as protos from "src/js/protos";
import { hoverOff, hoverOn, HoverState } from "src/redux/hover";
import { isSecondaryTenant } from "src/redux/tenants";
import { unique } from "src/util/arrays";
import { findChildrenOfType } from "src/util/find";
import {
  canShowMetric,
  configureUPlotLineChart,
  formatMetricData,
  formattedSeries,
} from "src/views/cluster/util/graphs";
import { MonitoringIcon } from "src/views/shared/components/icons/monitoring";
import {
  Axis,
  AxisProps,
  Metric,
  MetricProps,
  MetricsDataComponentProps,
  QueryTimeInfo,
} from "src/views/shared/components/metricQuery";

import "./linegraph.styl";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

export interface OwnProps extends MetricsDataComponentProps {
  isKvGraph?: boolean;
  title?: string;
  subtitle?: string;
  legend?: boolean;
  xAxis?: boolean;
  tooltip?: React.ReactNode;
  hoverOn?: typeof hoverOn;
  hoverOff?: typeof hoverOff;
  hoverState?: HoverState;
  preCalcGraphSize?: boolean;
  showMetricsInTooltip?: boolean;
}

export type LineGraphProps = OwnProps & WithTimezoneProps;

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
// InternalLinegraph is exported for testing.
export class InternalLineGraph extends React.Component<LineGraphProps, {}> {
  constructor(props: LineGraphProps) {
    super(props);

    this.setNewTimeRange = this.setNewTimeRange.bind(this);
  }

  static defaultProps: Partial<LineGraphProps> = {
    // Marking a graph as not being KV-related is opt-in.
    isKvGraph: true,
  };

  // axis is copied from the nvd3 LineGraph component above
  axis = createSelector(
    (props: { children?: React.ReactNode }) => props.children,
    children => {
      const axes: React.ReactElement<AxisProps>[] = findChildrenOfType(
        children as any,
        Axis,
      );
      if (axes.length === 0) {
        // eslint-disable-next-line no-console
        console.warn(
          "LineGraph requires the specification of at least one axis.",
        );
        return null;
      }
      if (axes.length > 1) {
        // eslint-disable-next-line no-console
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
      return findChildrenOfType(
        children as any,
        Metric,
      ) as React.ReactElement<MetricProps>[];
    },
  );

  // setNewTimeRange forces a
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
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    let newTimeScale: TimeScale = {
      ...findClosestTimeScale(defaultTimeScaleOptions, seconds),
      key: "Custom",
      windowSize: moment.duration(moment.unix(end).diff(moment.unix(start))),
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

  newTimeInfo(
    newTimeInfo: QueryTimeInfo,
    prevTimeInfo: QueryTimeInfo,
  ): boolean {
    if (newTimeInfo.start.compare(prevTimeInfo.start) !== 0) {
      return true;
    }
    if (newTimeInfo.end.compare(prevTimeInfo.end) !== 0) {
      return true;
    }
    if (newTimeInfo.sampleDuration.compare(prevTimeInfo.sampleDuration) !== 0) {
      return true;
    }

    return false;
  }

  hasDataPoints = (data: TSResponse): boolean => {
    let hasData = false;
    data?.results?.map(result => {
      if (result?.datapoints?.length > 0) {
        hasData = true;
      }
    });
    return hasData;
  };

  componentDidUpdate(prevProps: Readonly<LineGraphProps>) {
    if (
      !this.props.data?.results ||
      (prevProps.data === this.props.data &&
        this.u !== undefined &&
        !this.newTimeInfo(this.props.timeInfo, prevProps.timeInfo))
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
    const resultDatapoints = flatMap(fData, result =>
      result.values.map(dp => dp.value),
    );
    this.yAxisDomain = calculateYAxisDomain(axis.props.units, resultDatapoints);
    this.xAxisDomain = calculateXAxisDomain(
      util.NanoToMilli(this.props.timeInfo.start.toNumber()),
      util.NanoToMilli(this.props.timeInfo.end.toNumber()),
      this.props.timezone,
    );

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
    const {
      title,
      subtitle,
      tooltip,
      data,
      tenantSource,
      preCalcGraphSize,
      showMetricsInTooltip,
    } = this.props;
    let tt = tooltip;
    const addLines: React.ReactNode = tooltip ? (
      <>
        <br />
        <br />
      </>
    ) : null;
    // Extend tooltip to include metrics names
    if (showMetricsInTooltip) {
      const metrics = filter(data?.results, canShowMetric);
      if (metrics.length === 1) {
        tt = (
          <>
            {tt}
            {addLines}
            Metric: {metrics[0].query.name}
          </>
        );
      } else if (metrics.length > 1) {
        const metricNames = unique(metrics.map(m => m.query.name));
        tt = (
          <>
            {tt}
            {addLines}
            Metrics:
            <ul>
              {metricNames.map(m => (
                <li key={m}>{m}</li>
              ))}
            </ul>
          </>
        );
      }
    }

    if (!this.hasDataPoints(data) && isSecondaryTenant(tenantSource)) {
      return (
        <div className="linegraph-empty">
          <div className="header-empty">
            <Tooltip placement="bottom" title={tooltip}>
              <span className="title-empty">{title}</span>
            </Tooltip>
          </div>
          <div className="body-empty">
            <MonitoringIcon />
            <span className="body-text-empty">
              {"Metric is not currently available for this tenant."}
            </span>
          </div>
        </div>
      );
    }
    return (
      <Visualization
        title={title}
        subtitle={subtitle}
        tooltip={tt}
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

export default WithTimezone<OwnProps>(InternalLineGraph);
