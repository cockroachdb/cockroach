// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import Long from "long";
import moment from "moment";
import React from "react";
import { connect } from "react-redux";
import { createSelector } from "reselect";
import * as protos from "src/js/protos";
import { MetricsQuery, requestMetrics as requestMetricsAction } from "src/redux/metrics";
import { AdminUIState } from "src/redux/state";
import { MilliToNano } from "src/util/convert";
import { findChildrenOfType } from "src/util/find";
import { Metric, MetricProps, MetricsDataComponentProps, QueryTimeInfo } from "src/views/shared/components/metricQuery";

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
    let downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.AVG;

    // Compute derivative function.
    if (!_.isNil(metricProps.derivative)) {
      derivative = metricProps.derivative;
    } else if (metricProps.rate) {
      derivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.DERIVATIVE;
    } else if (metricProps.nonNegativeRate) {
      derivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative.NON_NEGATIVE_DERIVATIVE;
    }
    // Compute downsample function.
    if (!_.isNil(metricProps.downsampler)) {
      downsampler = metricProps.downsampler;
    } else if (metricProps.downsampleMax) {
      downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX;
    } else if (metricProps.downsampleMin) {
      downsampler = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MIN;
    }
    // Compute aggregation function.
    if (!_.isNil(metricProps.aggregator)) {
      sourceAggregator = metricProps.aggregator;
    } else if (metricProps.aggregateMax) {
      sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MAX;
    } else if (metricProps.aggregateMin) {
      sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.MIN;
    } else if (metricProps.aggregateAvg) {
      sourceAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator.AVG;
    }

    return {
      name: metricProps.name,
      sources: metricProps.sources || graphProps.sources || undefined,
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
}

/**
 * MetricsDataProviderProps is the complete set of properties which can be
 * provided to a MetricsDataProvider.
 */
type MetricsDataProviderProps = MetricsDataProviderConnectProps & MetricsDataProviderExplicitProps;

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
class MetricsDataProvider extends React.Component<MetricsDataProviderProps, {}> {
  private queriesSelector = createSelector(
    ({ children }: MetricsDataProviderProps) => children,
    (children) => {
      // MetricsDataProvider should contain only one direct child.
      const child: React.ReactElement<MetricsDataComponentProps> = React.Children.only(this.props.children);
      // Perform a simple DFS to find all children which are Metric objects.
      const selectors: React.ReactElement<MetricProps>[] = findChildrenOfType(children, Metric);
      // Construct a query for each found selector child.
      return _.map(selectors, (s) => queryFromProps(s.props, child.props));
    });

  private requestMessage = createSelector(
    (props: MetricsDataProviderProps) => props.timeInfo,
    this.queriesSelector,
    (timeInfo, queries) => {
      if (!timeInfo) {
        return undefined;
      }
      return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: timeInfo.start,
        end_nanos: timeInfo.end,
        sample_nanos: timeInfo.sampleDuration,
        queries,
      });
    });

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
    if (!nextRequest || !_.isEqual(nextRequest, request)) {
      requestMetrics(id, request);
    }
  }

  componentDidMount() {
    // Refresh nodes status query when mounting.
    this.refreshMetricsIfStale(this.props);
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
      if (data && request && _.isEqual(request.queries, this.requestMessage(this.props).queries)) {
        return data;
      }
    }
    return undefined;
  }

  render() {
    // MetricsDataProvider should contain only one direct child.
    const child = React.Children.only(this.props.children);
    const dataProps: MetricsDataComponentProps = {
      data: this.getData(),
      timeInfo: this.props.timeInfo,
    };
    return React.cloneElement(child as React.ReactElement<MetricsDataComponentProps>, dataProps);
  }
}

// timeInfoSelector converts the current global time window into a set of Long
// timestamps, which can be sent with requests to the server.
const timeInfoSelector = createSelector(
  (state: AdminUIState) => state.timewindow,
  (tw) => {
    if (!_.isObject(tw.currentWindow)) {
      return null;
    }
    return {
      start: Long.fromNumber(MilliToNano(tw.currentWindow.start.valueOf())),
      end: Long.fromNumber(MilliToNano(tw.currentWindow.end.valueOf())),
      sampleDuration: Long.fromNumber(MilliToNano(tw.scale.sampleSize.asMilliseconds())),
    };
  });

const current = () => {
  let now = moment();
  // Round to the nearest 10 seconds. There are 10000 ms in 10 s.
  now = moment(Math.floor(now.valueOf() / 10000) * 10000);
  return {
    start: Long.fromNumber(MilliToNano(now.clone().subtract(30, "s").valueOf())),
    end: Long.fromNumber(MilliToNano(now.valueOf())),
    sampleDuration: Long.fromNumber(MilliToNano(moment.duration(10, "s").asMilliseconds())),
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
  },
)(MetricsDataProvider);

export { MetricsDataProvider as MetricsDataProviderUnconnected, metricsDataProviderConnected as MetricsDataProvider };
