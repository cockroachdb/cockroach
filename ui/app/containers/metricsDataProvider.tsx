import * as React from "react";
import { createSelector } from "reselect";
import { connect } from "react-redux";
import _ from "lodash";
import Long from "long";

import * as protos from  "../js/protos";
import { AdminUIState } from "../redux/state";
import { queryMetrics, MetricsQuery } from "../redux/metrics";
import { MetricProps, Metric, MetricsDataComponentProps } from "../components/graphs";
import { findChildrenOfType } from "../util/find";
import { MilliToNano } from "../util/convert";
import { TimeSeriesQueryAggregator, TimeSeriesQueryDerivative } from "../util/protoEnums";

type TSQueryMessage = Proto2TypeScript.cockroach.ts.tspb.QueryMessage;

/**
 * queryFromProps is a helper method which generates a TimeSeries Query data
 * structure based on a MetricProps object.
 */
function queryFromProps(metricProps: MetricProps,
                        graphProps: MetricsDataComponentProps): TSQueryMessage {
    let derivative = TimeSeriesQueryDerivative.NONE;
    let sourceAggregator = TimeSeriesQueryAggregator.SUM;
    let downsampler = TimeSeriesQueryAggregator.AVG;

    // Compute derivative function.
    if (metricProps.rate) {
      derivative = TimeSeriesQueryDerivative.DERIVATIVE;
    } else if (metricProps.nonNegativeRate) {
      derivative = TimeSeriesQueryDerivative.NON_NEGATIVE_DERIVATIVE;
    }
    // Compute downsample function.
    if (metricProps.downsampleMax) {
      downsampler = TimeSeriesQueryAggregator.MAX;
    } else if (metricProps.downsampleMin) {
      downsampler = TimeSeriesQueryAggregator.MIN;
    }
    // Compute aggregation function.
    if (metricProps.aggregateMax) {
      sourceAggregator = TimeSeriesQueryAggregator.MAX;
    } else if (metricProps.aggregateMin) {
      sourceAggregator = TimeSeriesQueryAggregator.MIN;
    } else if (metricProps.aggregateAvg) {
      sourceAggregator = TimeSeriesQueryAggregator.AVG;
    }

    return new protos.cockroach.ts.tspb.Query({
      name: metricProps.name,
      sources: metricProps.sources || graphProps.sources || undefined,

      /**
       * HACK: This casting is related to the problem outlined in the comment on
       * TimeSeriesQueryAggregator above. TSQueryMessage expects values in terms of the
       * original enumeration (provided by Proto2Typescript), but the values are
       * not available in the SystemJS compiler. Values are cast *through*
       * number, as apparently direct casts between enumerations are forbidden.
       */
      downsampler: downsampler as number as Proto2TypeScript.cockroach.ts.tspb.TimeSeriesQueryAggregator,
      source_aggregator: sourceAggregator as number as Proto2TypeScript.cockroach.ts.tspb.TimeSeriesQueryAggregator,
      derivative: derivative as number as Proto2TypeScript.cockroach.ts.tspb.TimeSeriesQueryDerivative,
    });
}

// QueryTimeInfo is a convenience structure which can be used to pass important
// time information to a MetricsDataProvider.
export interface QueryTimeInfo {
  // The start time of the query, expressed as a unix timestamp in nanoseconds.
  start: Long;
  // The end time of the query, expressed as a unix timestamp in nanoseconds.
  end: Long;
  // The duration of individual samples in the query, expressed in nanoseconds.
  sampleDuration: Long;
}

/**
 * MetricsDataProviderConnectProps are the properties provided to a
 * MetricsDataProvider via the react-redux connect() system.
 */
interface MetricsDataProviderConnectProps {
  metrics: MetricsQuery;
  timeInfo: QueryTimeInfo;
  queryMetrics: typeof queryMetrics;
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
    (props: MetricsDataProviderProps & {children?: any}) => props.children,
    (children) => {
      // MetricsDataProvider should contain only one direct child.
      let child = React.Children.only(this.props.children) as
        React.ReactElement<MetricsDataComponentProps>;
      // Perform a simple DFS to find all children which are Metric objects.
      let selectors: React.ReactElement<MetricProps>[] = findChildrenOfType(children, Metric);
      // Construct a query for each found selector child.
      return _(selectors).map((s) => queryFromProps(s.props, child.props)).value();
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
    let request = this.requestMessage(props);
    if (!request) {
      return;
    }
    let { metrics, queryMetrics, id } = props;
    if (!metrics || !metrics.nextRequest || !_.isEqual(metrics.nextRequest, request)) {
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

  getData(props: MetricsDataProviderProps) {
    if (this.props.metrics) {
      let { data, request } = this.props.metrics;
      // Do not attach data if queries are not equivalent.
      if (data && _.isEqual(request.queries, this.requestMessage(props).queries)) {
        return data;
      }
    }
    return undefined;
  }

  render() {
    // MetricsDataProvider should contain only one direct child.
    let child = React.Children.only(this.props.children);
    let dataProps: MetricsDataComponentProps = {
      data: this.getData(this.props),
    };
    return React.cloneElement(child as React.ReactElement<MetricsDataComponentProps>, dataProps);
  }
}

// timeInfoSelector converts the current global time window into a set of Long
// timestamps, which can be sent with requests to the server.
let timeInfoSelector = createSelector(
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

// Connect the MetricsDataProvider class to redux state.
let metricsDataProviderConnected = connect(
  (state: AdminUIState, ownProps: MetricsDataProviderExplicitProps) => {
    return {
      metrics: state.metrics.queries[ownProps.id],
      timeInfo: timeInfoSelector(state),
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
