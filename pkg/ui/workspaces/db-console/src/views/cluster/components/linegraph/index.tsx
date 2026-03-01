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
import React, { useCallback, useEffect, useMemo, useRef } from "react";
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

import "./linegraph.scss";

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
  children?: React.ReactNode;
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

function hasTimeInfoChanged(
  newInfo: QueryTimeInfo,
  prevInfo: QueryTimeInfo,
): boolean {
  if (newInfo.start.compare(prevInfo.start) !== 0) return true;
  if (newInfo.end.compare(prevInfo.end) !== 0) return true;
  if (newInfo.sampleDuration.compare(prevInfo.sampleDuration) !== 0)
    return true;
  return false;
}

function hasDataPoints(data: TSResponse): boolean {
  return data?.results?.some(r => r?.datapoints?.length > 0) ?? false;
}

// LineGraph wraps the uPlot library into a React component.
// On first render with data, it constructs a uPlot instance and stores
// a ref to it. On subsequent data updates it either pushes new data
// into the existing instance or recreates it when the set of series changes.
// InternalLineGraph is exported for testing.
export function InternalLineGraph({
  title,
  subtitle,
  tooltip,
  preCalcGraphSize,
  showMetricsInTooltip,
  data,
  timeInfo,
  setMetricsFixedWindow,
  setTimeScale,
  adjustTimeScaleOnChange,
  tenantSource,
  children,
  timezone,
}: LineGraphProps): React.ReactElement {
  const el = useRef<HTMLDivElement>(null);
  const uRef = useRef<uPlot>(undefined);

  // yAxisDomain and xAxisDomain are recomputed when data changes.
  // uPlot options hold closures that read these refs, so updated
  // domains are used when redrawing the chart.
  const yAxisDomainRef = useRef<AxisDomain>(undefined);
  const xAxisDomainRef = useRef<AxisDomain>(undefined);

  // Track previous data and timeInfo for comparison (replaces prevProps).
  const prevDataRef = useRef<TSResponse>(undefined);
  const prevTimeInfoRef = useRef<QueryTimeInfo>(undefined);

  // Extract axis and metric child elements (memoized on children identity).
  const axisElement = useMemo(() => {
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
  }, [children]);

  const metricElements = useMemo(() => {
    return findChildrenOfType(
      children as any,
      Metric,
    ) as React.ReactElement<MetricProps>[];
  }, [children]);

  // setNewTimeRange is captured by uPlot hooks at chart creation time.
  // In the class version, `this.props` always gave the latest values;
  // here we use a ref so the function captured by uPlot always delegates
  // to the latest implementation (which closes over current props).
  const setNewTimeRangeRef = useRef<(s: number, e: number) => void>(null);
  setNewTimeRangeRef.current = (
    startMillis: number,
    endMillis: number,
  ): void => {
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
    if (adjustTimeScaleOnChange) {
      newTimeScale = adjustTimeScaleOnChange(newTimeScale, newTimeWindow);
    }
    setMetricsFixedWindow(newTimeWindow);
    setTimeScale(newTimeScale);
  };

  // Stable reference that delegates to the ref above.
  const setNewTimeRange = useCallback(
    (startMillis: number, endMillis: number) => {
      setNewTimeRangeRef.current(startMillis, endMillis);
    },
    [setNewTimeRangeRef],
  );

  // Destroy uPlot instance on unmount.
  useEffect(() => {
    return () => {
      if (uRef.current) {
        uRef.current.destroy();
        uRef.current = null;
      }
    };
  }, []);

  // Update chart when data, time info, or display dependencies change.
  // prevDataRef is still needed because the effect uses previous data to
  // compute prior series keys (deciding whether to setData() on the
  // existing uPlot or create a new instance).
  useEffect(() => {
    const prevData = prevDataRef.current;
    const prevTimeInfo = prevTimeInfoRef.current;
    prevDataRef.current = data;
    prevTimeInfoRef.current = timeInfo;

    if (
      !data?.results ||
      (prevData === data &&
        uRef.current !== undefined &&
        !(prevTimeInfo && hasTimeInfoChanged(timeInfo, prevTimeInfo)))
    ) {
      return;
    }

    const fData = formatMetricData(metricElements, data);
    const uPlotData = touPlot(fData, timeInfo?.sampleDuration);

    const resultDatapoints = flatMap(fData, result =>
      result.values.map(dp => dp.value),
    );
    yAxisDomainRef.current = calculateYAxisDomain(
      axisElement.props.units,
      resultDatapoints,
    );
    xAxisDomainRef.current = calculateXAxisDomain(
      util.NanoToMilli(timeInfo.start.toNumber()),
      util.NanoToMilli(timeInfo.end.toNumber()),
      timezone,
    );

    const prevKeys =
      prevData && prevData.results
        ? formatMetricData(metricElements, prevData).map(s => s.key)
        : [];
    const keys = fData.map(s => s.key);
    const sameKeys =
      keys.every(k => prevKeys.includes(k)) &&
      prevKeys.every(k => keys.includes(k));

    if (
      uRef.current && // we already created a uPlot instance
      prevData && // prior update had data as well
      sameKeys // prior update had same set of series identified by key
    ) {
      // The axis label option on uPlot doesn't accept
      // a function that recomputes the label, so we need
      // to manually update it in cases where we change
      // the scale (this happens on byte/time-based Y
      // axes where can change from MiB or KiB scales,
      // for instance).
      uRef.current.axes[1].label =
        axisElement.props.label +
        (yAxisDomainRef.current.label
          ? ` (${yAxisDomainRef.current.label})`
          : "");

      // Updates existing plot with new points.
      uRef.current.setData(uPlotData);
    } else {
      const options = configureUPlotLineChart(
        metricElements,
        axisElement,
        data,
        setNewTimeRange,
        () => xAxisDomainRef.current,
        () => yAxisDomainRef.current,
      );

      if (uRef.current) {
        uRef.current.destroy();
      }
      uRef.current = new uPlot(options, uPlotData, el.current);
    }
  }, [data, timeInfo, metricElements, axisElement, timezone, setNewTimeRange]);

  let tt = tooltip;
  const addLines: React.ReactNode = tooltip ? (
    <>
      <br />
      <br />
    </>
  ) : null;
  // Extend tooltip to include metrics names.
  if (showMetricsInTooltip) {
    const visibleMetrics = filter(data?.results, canShowMetric);
    if (visibleMetrics.length === 1) {
      tt = (
        <>
          {tt}
          {addLines}
          Metric: {visibleMetrics[0].query.name}
        </>
      );
    } else if (visibleMetrics.length > 1) {
      const metricNames = unique(visibleMetrics.map(m => m.query.name));
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

  if (!hasDataPoints(data) && isSecondaryTenant(tenantSource)) {
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
        <div ref={el} />
      </div>
    </Visualization>
  );
}

// Marking a graph as not being KV-related is opt-in. defaultProps is
// needed here (rather than a default parameter) because the parent reads
// element.props.isKvGraph externally for filtering.
InternalLineGraph.defaultProps = {
  isKvGraph: true,
};

export default WithTimezone<OwnProps>(InternalLineGraph);
