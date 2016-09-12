import * as React from "react";
import * as d3 from "d3";
import _ from "lodash";
import * as protos from "../js/protos";
import Long from "long";

type TSResponseMessage = Proto2TypeScript.cockroach.ts.tspb.TimeSeriesQueryResponseMessage;

/**
 * MetricProps reperesents the properties assigned to a selector component. A
 * selector describes a single time series that should be queried from the
 * server, along with some basic information on how that data should be rendered
 * in a graph context.
 */
export interface MetricProps {
  name: string;
  sources?: string[];
  title?: string;
  rate?: boolean;
  nonNegativeRate?: boolean;
  aggregateMax?: boolean;
  aggregateMin?: boolean;
  aggregateAvg?: boolean;
  downsampleMax?: boolean;
  downsampleMin?: boolean;
}

/**
 * Metric is a React component which describes a selector. This exists as a
 * component for convenient syntax, and should not be rendered directly; rather,
 * a renderable component will contain metrics, but will use them
 * only informationally within rendering them.
 */
export class Metric extends React.Component<MetricProps, {}> {
  render(): React.ReactElement<any> {
    throw new Error("Component <Metric /> should never render.");
  }
};

/**
 * AxisProps represents the properties of a renderable graph axis.
 */
export interface AxisProps {
  label?: string;
  format?: (n: number) => string;
  range?: number[];
  yLow?: number;
  yHigh?: number;
}

// SeenTimestamps is used to track which timestamps have been included in the
// current dataset and which are missing
interface SeenTimestamps {
  [key: number]: boolean;
}

/**
 * Axis is a React component which describes a renderable axis on a graph. This
 * exists as a component for convenient syntax, and should not be rendered
 * directly; rather, a renderable component will contain axes, but use them only
 * informationally without rendering them.
 */
export class Axis extends React.Component<AxisProps, {}> {
  static defaultProps: AxisProps = {
    yLow: 0,
    yHigh: 1,
  };

  render(): React.ReactElement<any> {
    throw new Error("Component <Axis /> should never render.");
  }
}

/**
 * AxisDomain is a helper class used to compute the domain of a set of numbers.
 * It can also be used to
 */
class AxisDomain {
  min: number;
  max: number;
  constructor() {
    this.min = Infinity;
    this.max = -Infinity;
  }

  domain(): [number, number] {
    return [this.min, this.max];
  }

  addPoints(values: number[]) {
    this.min = Math.min(this.min, ...values);
    this.max = Math.max(this.max, ...values);
  }

  ticks(transform: (n: number) => any = _.identity): number[] {
    return _.map(_.uniq([this.min, (this.min + this.max) / 2, this.max]), transform);
  }
}

// Global set of d3 colors.
let colors: d3.scale.Ordinal<string, string> = d3.scale.category10();

/**
 * getTimestamps is a helper function that takes graph data from the server and
 * returns a SeenTimestamps object with all the values set to false. This object
 * is used to track missing timestamps for each individual dataset.
 */
function getTimestamps(metrics: React.ReactElement<MetricProps>[], data: TSResponseMessage): SeenTimestamps {
  return _(metrics)
     // Get all the datapoints from all series in a single array.
    .flatMap((s, idx) => data.results[idx].datapoints)
    // Create a map keyed by the datapoint timestamps.
    .keyBy((d) => d.timestamp_nanos.toNumber())
    // Set all values to false, since we only want the keys.
    .mapValues(() => false)
    // Unwrap the lodash object.
    .value();
}

/**
 * ProcessDataPoints is a helper function to process graph data from the server
 * into a format appropriate for display on an NVD3 graph. This includes the
 * computation of domains and ticks for all axes.
 */
export function ProcessDataPoints(metrics: React.ReactElement<MetricProps>[],
                                  axis: React.ReactElement<AxisProps>,
                                  data: TSResponseMessage) {
  let yAxisDomain = new AxisDomain();
  let xAxisDomain = new AxisDomain();

  let formattedData: any[] = [];

  // timestamps has a key for all the timestamps present across all datasets
  let timestamps = getTimestamps(metrics, data);

  _.each(metrics, (s, idx) => {
    let result = data.results[idx];
    if (result) {
      yAxisDomain.addPoints(_.map(result.datapoints, (dp) => dp.value));
      xAxisDomain.addPoints(_.map(result.datapoints, (dp) => dp.timestamp_nanos.toNumber()));

      let datapoints = _.clone(result.datapoints);
      let seenTimestamps: SeenTimestamps = _.clone(timestamps);
      _.each(datapoints, (d) => seenTimestamps[d.timestamp_nanos.toNumber()] = true);

      _.each(seenTimestamps, (seen, ts) => {
        if (!seen) {
          datapoints.push(new protos.cockroach.ts.tspb.TimeSeriesDatapoint({
            timestamp_nanos: Long.fromString(ts),
            value: null,
          }));
        }
      });

      formattedData.push({
        values: datapoints || [],
        key: s.props.title || s.props.name,
        color: colors(s.props.name),
        area: true,
        fillOpacity: .1,
      });
    }
  });

  if (_.isNumber(axis.props.yLow)) {
    yAxisDomain.addPoints([axis.props.yLow]);
  }
  if (_.isNumber(axis.props.yHigh)) {
    yAxisDomain.addPoints([axis.props.yHigh]);
  }

  return {
    formattedData,
    yAxisDomain,
    xAxisDomain,
  };
}

// MetricsDataComponentProps is an interface that should be implemented by any
// components directly contained by a MetricsDataProvider. It is used by a
// MetricsDataProvider to pass query data to its contained component.
export interface MetricsDataComponentProps {
  data?: TSResponseMessage;
  // Allow graphs to declare a single source for all metrics. This is a
  // convenient syntax for a common use case where all metrics on a graph are
  // are from the same source set.
  sources?: string[];
}

// TextGraph is a proof-of-concept component used to demonstrate that
// MetricsDataProvider is working correctly. Used in tests.
export class TextGraph extends React.Component<MetricsDataComponentProps, {}> {
  render() {
    return <div>{
      (this.props.data && this.props.data.results) ? this.props.data.results.join(":") : ""
    }</div>;
  }
}
