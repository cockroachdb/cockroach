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
import React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";

import { PayloadAction } from "src/interfaces/action";
import * as protos from "src/js/protos";
import { refreshSettings } from "src/redux/apiReducers";
import {
  selectResolution10sStorageTTL,
  selectResolution30mStorageTTL,
} from "src/redux/clusterSettings";
import {
  MetricsQuery,
  requestMetrics as requestMetricsAction,
} from "src/redux/metrics";
import { AdminUIState } from "src/redux/state";
import { adjustTimeScale, selectMetricsTime } from "src/redux/timeScale";
import { findChildrenOfType } from "src/util/find";
import {
  Metric,
  MetricProps,
  MetricsDataComponentProps,
  QueryTimeInfo,
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
 * MetricsDataProviderConnectProps are the properties provided to a
 * MetricsDataProvider via the react-redux connect() system.
 */
interface MetricsDataProviderConnectProps {
  metrics: MetricsQuery;
  timeInfo: QueryTimeInfo;
  requestMetrics: typeof requestMetricsAction;
  refreshNodeSettings: typeof refreshSettings;
  setMetricsFixedWindow?: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale?: (ts: TimeScale) => PayloadAction<TimeScale>;
  history?: History;
}

/**
 * MetricsDataProviderExplicitProps are the properties provided explicitly to a
 * MetricsDataProvider object via React (i.e. setting an attribute in JSX).
 */
interface MetricsDataProviderExplicitProps {
  id: string;
  // If current is true, uses the current time instead of the global timewindow.
  current?: boolean;
  children?: React.ReactElement<{}>;
  adjustTimeScaleOnChange?: (
    curTimeScale: TimeScale,
    timeWindow: TimeWindow,
  ) => TimeScale;
}

/**
 * MetricsDataProviderProps is the complete set of properties which can be
 * provided to a MetricsDataProvider.
 */
type MetricsDataProviderProps = MetricsDataProviderConnectProps &
  MetricsDataProviderExplicitProps;

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
class MetricsDataProvider extends React.Component<
  MetricsDataProviderProps,
  {}
> {
  private queriesSelector = createSelector(
    ({ children }: MetricsDataProviderProps) => children,
    children => {
      // MetricsDataProvider should contain only one direct child.
      const child: React.ReactElement<MetricsDataComponentProps> =
        React.Children.only(this.props.children);
      // Perform a simple DFS to find all children which are Metric objects.
      const selectors: React.ReactElement<MetricProps>[] = findChildrenOfType(
        children,
        Metric,
      );
      // Construct a query for each found selector child.
      return map(selectors, s => queryFromProps(s.props, child.props));
    },
  );

  private requestMessage = createSelector(
    (props: MetricsDataProviderProps) => props.timeInfo,
    this.queriesSelector,
    (timeInfo, queries) => {
      if (!timeInfo || queries.length === 0) {
        return undefined;
      }
      return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: timeInfo.start,
        end_nanos: timeInfo.end,
        sample_nanos: timeInfo.sampleDuration,
        queries,
      });
    },
  );

  /**
   * Refresh nodes status query when props are changed; this will immediately
   * trigger a new request if the previous query is no longer valid.
   */
  refreshMetricsIfStale(props: MetricsDataProviderProps) {
    const request = this.requestMessage(props);
    if (!request) {
      return;
    }
    const { metrics, requestMetrics, id } = props;
    const nextRequest = metrics && metrics.nextRequest;
    if (!nextRequest || !isEqual(nextRequest, request)) {
      requestMetrics(id, request);
    }
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refreshMetricsIfStale(this.props);
    this.props.refreshNodeSettings();
  }

  componentDidUpdate() {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    this.refreshMetricsIfStale(this.props);
  }

  getData() {
    if (this.props.metrics) {
      const { data, request } = this.props.metrics;
      // Do not attach data if queries are not equivalent.
      if (
        data &&
        request &&
        isEqual(request.queries, this.requestMessage(this.props).queries)
      ) {
        return data;
      }
    }
    return undefined;
  }

  render() {
    const { adjustTimeScaleOnChange } = this.props;
    // MetricsDataProvider should contain only one direct child.
    const child = React.Children.only(this.props.children);
    const dataProps: MetricsDataComponentProps = {
      data: this.getData(),
      timeInfo: this.props.timeInfo,
      setMetricsFixedWindow: this.props.setMetricsFixedWindow,
      setTimeScale: this.props.setTimeScale,
      history: this.props.history,
      adjustTimeScaleOnChange,
    };
    return React.cloneElement(
      child as React.ReactElement<MetricsDataComponentProps>,
      dataProps,
    );
  }
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

// Connect the MetricsDataProvider class to redux state.
const metricsDataProviderConnected = connect(
  (state: AdminUIState, ownProps: MetricsDataProviderExplicitProps) => {
    return {
      metrics: state.metrics.queries[ownProps.id],
      timeInfo: ownProps.current ? current() : timeInfoSelector(state),
    };
  },
  {
    requestMetrics: requestMetricsAction,
    refreshNodeSettings: refreshSettings,
  },
)(MetricsDataProvider);

export {
  MetricsDataProvider as MetricsDataProviderUnconnected,
  metricsDataProviderConnected as MetricsDataProvider,
};
