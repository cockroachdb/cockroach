// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  util,
  TimeWindow,
  TimeScale,
  findClosestTimeScale,
  defaultTimeScaleOptions,
} from "@cockroachlabs/cluster-ui";
import { History } from "history";
import isEqual from "lodash/isEqual";
import isNil from "lodash/isNil";
import isObject from "lodash/isObject";
import map from "lodash/map";
import Long from "long";
import moment from "moment-timezone";
import React, { useEffect, useMemo } from "react";
import { useSelector, useDispatch } from "react-redux";
import { createSelector } from "reselect";

import { PayloadAction } from "src/interfaces/action";
import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import {
  selectResolution10sStorageTTL,
  selectResolution30mStorageTTL,
} from "src/redux/clusterSettings";
import { requestMetrics as requestMetricsAction } from "src/redux/metrics";
import { AdminUIState } from "src/redux/state";
import { adjustTimeScale, selectMetricsTime } from "src/redux/timeScale";
import { findChildrenOfType } from "src/util/find";
import {
  Metric,
  MetricProps,
  MetricsDataComponentProps,
} from "src/views/shared/components/metricQuery";

/**
 * queryFromProps is a helper method which generates a TimeSeries Query data
 * structure based on a MetricProps object.
 */
function queryFromProps(
  metricProps: MetricProps,
  graphProps: MetricsDataComponentProps,
): protos.cockroach.ts.tspb.IQuery {
  let derivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.NONE;
  let sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.SUM;
  let downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX;

  // Compute derivative function.
  if (!isNil(metricProps.derivative)) {
    derivative = metricProps.derivative;
  } else if (metricProps.rate) {
    derivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.DERIVATIVE;
  } else if (metricProps.nonNegativeRate) {
    derivative =
      protos.cockroach.ts.tspb.TimeSeriesQueryDerivative
        .NON_NEGATIVE_DERIVATIVE;
  }
  // Compute downsample function.
  if (!isNil(metricProps.downsampler)) {
    downsampler = metricProps.downsampler;
  } else if (metricProps.downsampleMax) {
    downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX;
  } else if (metricProps.downsampleMin) {
    downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MIN;
  }
  // Compute aggregation function.
  if (!isNil(metricProps.aggregator)) {
    sourceAggregator = metricProps.aggregator;
  } else if (metricProps.aggregateMax) {
    sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX;
  } else if (metricProps.aggregateMin) {
    sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MIN;
  } else if (metricProps.aggregateAvg) {
    sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.AVG;
  }
  const tenantSource =
    metricProps.tenantSource || graphProps.tenantSource || undefined;
  return {
    name: metricProps.name,
    sources: metricProps.sources || graphProps.sources || undefined,
    tenant_id: tenantSource ? { id: Long.fromString(tenantSource) } : undefined,
    downsampler: downsampler,
    source_aggregator: sourceAggregator,
    derivative: derivative,
  };
}

/**
 * MetricsDataProviderProps are the properties provided explicitly to a
 * MetricsDataProvider object via React (i.e. setting an attribute in JSX).
 */
interface MetricsDataProviderProps {
  id: string;
  // If current is true, uses the current time instead of the global timewindow.
  current?: boolean;
  children?: React.ReactElement<{}>;
  adjustTimeScaleOnChange?: (
    curTimeScale: TimeScale,
    timeWindow: TimeWindow,
  ) => TimeScale;
  setMetricsFixedWindow?: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale?: (ts: TimeScale) => PayloadAction<TimeScale>;
  history?: History;
}

// timeInfoSelector converts the current global time window into a set of Long
// timestamps, which can be sent with requests to the server.
const timeInfoSelector = createSelector(
  selectResolution10sStorageTTL,
  selectResolution30mStorageTTL,
  selectMetricsTime,
  (sTTL, mTTL, metricsTime) => {
    if (!isObject(metricsTime.currentWindow)) {
      return null;
    }
    const { start: startMoment, end: endMoment } = metricsTime.currentWindow;
    const start = startMoment.valueOf();
    const end = endMoment.valueOf();
    const syncedScale = findClosestTimeScale(
      defaultTimeScaleOptions,
      util.MilliToSeconds(end - start),
    );
    // Call adjustTimeScale to handle the case where the sample size
    // (also known as resolution) is too small for a start and end time
    // that is before the data's ttl.
    const adjusted = adjustTimeScale(
      { ...syncedScale, fixedWindowEnd: false },
      { start: startMoment, end: endMoment },
      sTTL,
      mTTL,
    );

    return {
      start: Long.fromNumber(util.MilliToNano(start)),
      end: Long.fromNumber(util.MilliToNano(end)),
      sampleDuration: Long.fromNumber(
        util.MilliToNano(adjusted.timeScale.sampleSize.asMilliseconds()),
      ),
    };
  },
);

const current = () => {
  let now = moment();
  // Round to the nearest 10 seconds. There are 10000 ms in 10 s.
  now = moment(Math.floor(now.valueOf() / 10000) * 10000);
  return {
    start: Long.fromNumber(
      util.MilliToNano(now.clone().subtract(30, "s").valueOf()),
    ),
    end: Long.fromNumber(util.MilliToNano(now.valueOf())),
    sampleDuration: Long.fromNumber(
      util.MilliToNano(moment.duration(10, "s").asMilliseconds()),
    ),
  };
};

/**
 * MetricsDataProvider is a container which manages query data for a renderable
 * component. For example, MetricsDataProvider may contain a "LineGraph"
 * component; the metric set becomes responsible for querying the server
 * required by that LineGraph.
 *
 * <MetricsDataProvider id="series-x-graph">
 *  <LineGraph data="[]">
 *    <Axis label="Series X over time.">
 *      <Metric title="" name="series.x" sources="node.1" />
 *    </Axis>
 *  </LineGraph>
 * </MetricsDataProvider>;
 *
 * Each MetricsDataProvider must have an ID field, which identifies this
 * particular set of metrics to the metrics query reducer. Currently queries
 * metrics from the reducer will be provided to the metric set via the
 * react-redux connection.
 *
 * Additionally, each MetricsDataProvider has a single, externally set TimeSpan
 * property, that determines the window over which time series should be
 * queried. This property is also currently intended to be set via react-redux.
 */
function MetricsDataProvider({
  id,
  current: isCurrent,
  children,
  adjustTimeScaleOnChange,
  setMetricsFixedWindow,
  setTimeScale,
  history,
}: MetricsDataProviderProps): React.ReactElement {
  const dispatch = useDispatch();
  const metrics = useSelector(
    (state: AdminUIState) => state.metrics.queries[id],
  );
  const timeInfo = useSelector((state: AdminUIState) =>
    isCurrent ? current() : timeInfoSelector(state),
  );

  // MetricsDataProvider should contain only one direct child.
  const child = React.Children.only(children);

  // Compute the time series queries from child Metric components.
  const queries = useMemo(() => {
    const selectors: React.ReactElement<MetricProps>[] = findChildrenOfType(
      children,
      Metric,
    );
    return map(selectors, s =>
      queryFromProps(
        s.props,
        (child as React.ReactElement<MetricsDataComponentProps>).props,
      ),
    );
  }, [children, child]);

  // Build the request message from timeInfo and queries.
  const requestMsg = useMemo(() => {
    if (!timeInfo || queries.length === 0) {
      return undefined;
    }
    return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
      start_nanos: timeInfo.start,
      end_nanos: timeInfo.end,
      sample_nanos: timeInfo.sampleDuration,
      queries,
    });
  }, [timeInfo, queries]);

  // Refresh metrics if the current request is stale. Runs on mount and
  // whenever the request message, metrics state, or id changes.
  useEffect(() => {
    if (!requestMsg) {
      return;
    }
    const nextRequest = metrics && metrics.nextRequest;
    if (!nextRequest || !isEqual(nextRequest, requestMsg)) {
      dispatch(requestMetricsAction(id, requestMsg));
    }
  }, [requestMsg, metrics, dispatch, id]);

  // Refresh node settings on mount.
  useEffect(() => {
    dispatch(refreshSettings());
  }, [dispatch]);

  // Compute data to pass to the child component. Only attach data if the
  // stored queries match the current request queries.
  const data = useMemo(() => {
    if (metrics) {
      const { data: metricsData, request } = metrics;
      if (
        metricsData &&
        request &&
        requestMsg &&
        isEqual(request.queries, requestMsg.queries)
      ) {
        return metricsData;
      }
    }
    return undefined;
  }, [metrics, requestMsg]);

  const dataProps: MetricsDataComponentProps = {
    data,
    timeInfo,
    setMetricsFixedWindow,
    setTimeScale,
    history,
    adjustTimeScaleOnChange,
  };
  return React.cloneElement(
    child as React.ReactElement<MetricsDataComponentProps>,
    dataProps,
  );
}

export { MetricsDataProvider };
