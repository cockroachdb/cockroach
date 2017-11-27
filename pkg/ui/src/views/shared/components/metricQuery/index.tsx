/**
 * MetricQuery Components
 *
 * These react-like components are intended to express metric queries for graphs
 * in a declarative, HTML-like syntax.  For example, a query for a graph that
 * displays the non-negative rate of change of two metrics on a shared axis:
 *
 * <Axis units={AxisUnits.Duration}>
 *  <Metric name="cr.node.sys.cpu.user.ns" title="User CPU Time" nonNegativeRate />
 *  <Metric name="cr.node.sys.cpu.sys.ns" title="Sys CPU Time" nonNegativeRate />
 * </Axis>
 *
 * This information is used to construct a query to the backend for metrics; it
 * is also used by the parent component to render the query response correctly.
 *
 * While these are react components, they are not intended to be rendered
 * directly and will throw an error if rendered. Instead, it is intended that
 * parent components to read the information expressed by these components and
 * combine it with the result of a query to create some renderable output.
 */

import React from "react";
import * as protos from  "src/js/protos";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

/**
 * AxisUnits is an enumeration used to specify the type of units being displayed
 * on an Axis.
 */
export enum AxisUnits {
  /**
   * Units are a simple count.
   */
  Count,
  /**
   * Units are a count of bytes.
   */
  Bytes,
  /**
   * Units are durations expressed in nanoseconds.
   */
  Duration,
}

/**
 * AxisProps represents the properties of an Axis being specified as part of a
 * query for metrics.
 */
export interface AxisProps {
  label?: string;
  format?: (n: number) => string;
  range?: number[];
  units?: AxisUnits;
}

/**
 * Axis is a React component which describes an Axis of a metrics query.
 *
 * This component should not be rendered directly; rather, a renderable
 * component should contain axes as children and use them only informationally
 * without rendering them.
 */
export class Axis extends React.Component<AxisProps, {}> {
  static defaultProps: AxisProps = {
    units: AxisUnits.Count,
  };

  render(): React.ReactElement<any> {
    throw new Error("Component <Axis /> should never render.");
  }
}

/**
 * MetricProps reperesents the properties of a Metric being selected as part of
 * a query.
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
 * Metric is a React component which describes a Metric in a metrics query.
 *
 * This component should not be rendered directly; rather, a renderable
 * component should contain axes as children and use them only informationally
 * without rendering them.
 */
export class Metric extends React.Component<MetricProps, {}> {
  render(): React.ReactElement<any> {
    throw new Error("Component <Metric /> should never render.");
  }
}

/**
 * QueryTimeInfo is a convenience structure which contains information about
 * the time range of a metrics query.
 */
export interface QueryTimeInfo {
  // The start time of the query, expressed as a unix timestamp in nanoseconds.
  start: Long;
  // The end time of the query, expressed as a unix timestamp in nanoseconds.
  end: Long;
  // The duration of individual samples in the query, expressed in nanoseconds.
  sampleDuration: Long;
}

/**
 * MetricsDataComponentProps is an interface that should be implemented by any
 * components expecting to receive a metrics query result.
 */
export interface MetricsDataComponentProps {
  data?: TSResponse;
  timeInfo?: QueryTimeInfo;
  // Allow graphs to declare a single source for all metrics. This is a
  // convenient syntax for a common use case where all metrics on a graph are
  // are from the same source set.
  sources?: string[];
}
