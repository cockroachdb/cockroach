import * as React from "react";
import _ from "lodash";
import * as protos from "../js/protos";
import Long from "long";

import { MetricProps } from "./metric";

type TSResponseMessage = Proto2TypeScript.cockroach.ts.tspb.TimeSeriesQueryResponseMessage;

// Global set of colors for graph series.
export let seriesPalette = [
  "#5F6C87", "#F2BE2C", "#F16969", "#4E9FD1", "#49D990", "#D77FBF", "#87326D", "#A3415B",
  "#B59153", "#C9DB6D", "#203D9B", "#748BF2", "#91C8F2", "#FF9696", "#EF843C", "#DCCD4B",
];

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
 */
class AxisDomain {
  min: number = Infinity;
  max: number = -Infinity;
  stackedSum: { [key: number]: number } = {};
  tickCount: number;
  maxMinTicks: boolean;

  constructor(tickCount: number = 3, maxMinTicks: boolean = true) {
    this.tickCount = tickCount;
    this.maxMinTicks = maxMinTicks;
  }

  // domain returns the current min and max as an array.
  domain(): [number, number] {
    return [this.min, this.max];
  }

  // addPoints adds values. It will extend the max/min of the domain if any
  // values are lower/higher than the current max/min respectively.
  addPoints(values: number[]) {
    this.min = Math.min(this.min, ...values);
    this.max = Math.max(this.max, ...values);
  }
  // addStacked adds keyed values. It sums the values by key and then extends
  // the max/min of the domain if any sum is higher/lower than the current
  // max/min respectively.
  addStackedPoints(keyedValues: {key: number, value: number}[]) {
    _.each(keyedValues, (v) => {
      this.stackedSum[v.key] = (this.stackedSum[v.key] || 0) + v.value;
    });

    this.min = _.min(_.values<number>(this.stackedSum));
    this.max = _.max(_.values<number>(this.stackedSum));
  }

  // ticks computes tick values for a graph given the current max/min and
  // tickCount.
  ticks(transform: (n: number) => any = _.identity): number[] {
    let increment = (this.max - this.min) / (this.tickCount + 1);
    let tix: number[] = [];
    if (this.maxMinTicks) {
      tix.push(this.min);
    }
    for (let i = 0; i < this.tickCount; i++) {
      tix.push(this.min + increment * (i + 1));
    }
    if (this.maxMinTicks) {
      tix.push(this.max);
    }
    return _.map(_.uniq(tix), transform);
  }
}

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
                                  data: TSResponseMessage,
                                  stacked = false) {
  let yAxisDomain = new AxisDomain(3);
  let xAxisDomain = new AxisDomain(10, false);

  let formattedData: any[] = [];

  // timestamps has a key for all the timestamps present across all datasets
  let timestamps = getTimestamps(metrics, data);

  _.each(metrics, (s, idx) => {
    let result = data.results[idx];
    if (result) {
      if (!stacked) {
        yAxisDomain.addPoints(_.map(result.datapoints, (dp) => dp.value));
      } else {
        yAxisDomain.addStackedPoints(_.map(result.datapoints, (dp) => { return { key: dp.timestamp_nanos.toNumber(), value: dp.value }; }));
      }
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
