/// <reference path="../../typings/main.d.ts" />

import * as React from "react";
import { createSelector } from "reselect";
import { connect } from "react-redux";
import _ = require("lodash");
import Long = require("long");

import * as protos from  "../js/protos";
import { queryMetrics, MetricsQuery } from "../redux/metrics";
import * as timewindow from "../redux/timewindow";
import { MetricProps, Metric, MetricsDataComponentProps } from "../components/graphs";
import { findChildrenOfType } from "../util/find";
import { MilliToNano } from "../util/convert";

type TSQueryMessage = cockroach.ts.QueryMessage;
type TSRequestMessage = cockroach.ts.TimeSeriesQueryRequestMessage;
type TSResponseMessage = cockroach.ts.TimeSeriesQueryResponseMessage;

/**
 * TimeSeriesQueryAggregator is an enumeration used by Cockroach's time series
 * query system, used to select an aggregator function.
 * This must be kept manually in sync with the same enumeration in `generated/protos.d.ts`.
 *
 * HACK
 *
 * This enumeration is copied due to an incompatibility between two tools in our
 * system:
 *
 * 1. The systemJS typescript loader (frankwallis/plugin-typescript) compiles
 * files using typescript's "single file compilation" mode, which is unable to
 * resolve ambiently declared const enums:
 *
 *     https://github.com/frankwallis/plugin-typescript/issues/89
 *
 * 2. The Proto2Typescript generator (SINTEF-9012/Proto2TypeScript) outputs
 * enumerations as ambiently declared const enums.
 *
 * Unfortunately, it is not trivial to change either of these behaviors; the
 * plugin-typescript behavior is unfixable (fundamentally incompatible with its
 * current basic strategy), while the Proto2Typescript generated file would need
 * to be changed dramatically; specifically, it would need generate an
 * importable module format (rather than its current design of declaring ambient
 * global objects).
 */
const enum TimeSeriesQueryAggregator {
  AVG = 1,
  SUM = 2,
  MAX = 3,
  MIN = 4
}

/**
 * TimeSeriesQueryDerivative is an enumeration used by Cockroach's time series
 * query system, used to select an derivated function.
 * This must be kept manually in sync with the same enumeration in `generated/protos.d.ts`.
 */
const enum TimeSeriesQueryDerivative {
  NONE = 0,
  DERIVATIVE = 1,
  NON_NEGATIVE_DERIVATIVE = 2
}

/**
 * queryFromProps is a helper method which generates a TimeSeries Query data
 * structure based on a MetricProps object.
 */
function queryFromProps(props: MetricProps): TSQueryMessage {
    let derivative = TimeSeriesQueryDerivative.NONE;
    let sourceAggregator = TimeSeriesQueryAggregator.SUM;
    let downsampler = TimeSeriesQueryAggregator.AVG;

    // Compute derivative function.
    if (props.rate) {
      derivative = TimeSeriesQueryDerivative.DERIVATIVE;
    } else if (props.nonNegativeRate) {
      derivative = TimeSeriesQueryDerivative.NON_NEGATIVE_DERIVATIVE;
    }
    // Compute downsample function.
    if (props.downsampleMax) {
      downsampler = TimeSeriesQueryAggregator.MAX;
    } else if (props.downsampleMin) {
      downsampler = TimeSeriesQueryAggregator.MIN;
    }
    // Compute aggregation function.
    if (props.aggregateMax) {
      sourceAggregator = TimeSeriesQueryAggregator.MAX;
    } else if (props.aggregateMin) {
      sourceAggregator = TimeSeriesQueryAggregator.MIN;
    }

    return new protos.cockroach.ts.Query({
      name: props.name,
      sources: props.sources || undefined,

      /**
       * HACK: This casting is related to the problem outlined in the comment on
       * TimeSeriesQueryAggregator above. TSQueryMessage expects values in terms of the
       * original enumeration (provided by Proto2Typescript), but the values are
       * not available in the SystemJS compiler. Values are cast *through*
       * number, as apparently direct casts between enumerations are forbidden.
       */
      downsampler: downsampler as number as cockroach.ts.TimeSeriesQueryAggregator,
      source_aggregator: sourceAggregator as number as cockroach.ts.TimeSeriesQueryAggregator,
      derivative: derivative as number as cockroach.ts.TimeSeriesQueryDerivative,
    });
}

/**
 * MetricsDataProviderConnectProps are the properties provided to a
 * MetricsDataProvider via the react-redux connect() system.
 */
interface MetricsDataProviderConnectProps {
  metrics: MetricsQuery;
  timeSpan: Long[];
  queryMetrics(id: string, request: TSRequestMessage): void;
}

/**
 * MetricsDataProviderExplicitProps are the properties provided explicitly to a
 * MetricsDataProvider object via React (i.e. setting an attribute in JSX).
 */
interface MetricsDataProviderExplicitProps {
  id: string;
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
 * <MetricsSet id="series-x-graph">
 *  <LineGraph data="[]">
 *    <Axis label="Series X over time.">
 *      <Avg title="" name="series.x" sources="node.1">
 *    </Axis>
 *  </LineGraph>
 * </MetricsSet>;
 *
 * Each MetricSet must have an ID field, which identifies this particular set to
 * the metrics query reducer. Currently queries metrics from the reducer will be
 * provided to the metric set via the react-redux connection.
 *
 * Additionally, each MetricSet has a single, externally set TimeSpan property,
 * that determines the window over which time series should be queried. This
 * property is also currently intended to be set via react-redux.
 */
class MetricsDataProvider extends React.Component<MetricsDataProviderProps, {}> {
  private queriesSelector = createSelector(
    (props: {children?: any}) => props.children,
    (children) => {
      // Perform a simple DFS to find all children which are Metric objects.
      let selectors: React.ReactElement<MetricProps>[] = findChildrenOfType(children, Metric);
      // Construct a query for each found selector child.
      return _(selectors).map((s) => queryFromProps(s.props)).value();
    });

  /**
   * Refresh nodes status query when props are changed; this will immediately
   * trigger a new request if the previous query is no longer valid.
   */
  refreshMetricsIfStale(props: MetricsDataProviderProps) {
    let { metrics, timeSpan, queryMetrics, id } = props;
    if (!timeSpan) {
      return;
    }
    if (!metrics || !metrics.request ||
        !timeSpanMatch(metrics.request, timeSpan)) {
      let request = new protos.cockroach.ts.TimeSeriesQueryRequest({
        start_nanos: timeSpan[0],
        end_nanos: timeSpan[1],
        queries: this.queriesSelector(props),
      });
      queryMetrics(id, request);
    }
  }

  componentWillMount() {
    // Refresh nodes status query when mounting.
    this.refreshMetricsIfStale(this.props);
  }

  componentWillReceiveProps(props: MetricsDataProviderProps) {
    // Refresh nodes status query when props are received; this will immediately
    // trigger a new request if previous results are invalidated.
    this.refreshMetricsIfStale(props);
  }

  render() {
    // MetricsDataProvider should contain only one direct child.
    let child = React.Children.only(this.props.children);
    let dataProps: MetricsDataComponentProps = { data: this.props.metrics && this.props.metrics.data };
    return React.cloneElement(child as React.ReactElement<MetricsDataComponentProps>, dataProps);
  }
}

// timeSpanSelector converts the current global time window into a pair of Long
// values, which can be sent with requests to the server.
let timeSpanSelector = createSelector(
  (state: any) => state.timewindow as timewindow.TimeWindowState,
  (tw) => {
    if (!_.isObject(tw.currentWindow)) {
      return null;
    }
    return [
      Long.fromNumber(MilliToNano(tw.currentWindow.start.valueOf())),
      Long.fromNumber(MilliToNano(tw.currentWindow.end.valueOf())),
    ];
  });

// Connect the MetricsDataProvider class to redux state.
let metricsDataProviderConnected = connect(
  (state: any, ownProps: MetricsDataProviderExplicitProps) => {
    return {
      metrics: state.metrics.queries[ownProps.id],
      timeSpan: timeSpanSelector(state),
    };
  },
  {
    queryMetrics: queryMetrics,
  }
)(MetricsDataProvider);

export {
  // Export original, unconnected MetricsDataProvider for effective unit
  // testing.
  MetricsDataProvider as MetricsDataProviderUnconnected,
  metricsDataProviderConnected as MetricsDataProvider
};

// Utility function to help determine that the requested time span has changed.
function timeSpanMatch(request: TSRequestMessage, timeSpan: Long[]): boolean {
  return request.start_nanos.equals(timeSpan[0]) && request.end_nanos.equals(timeSpan[1]);
}
