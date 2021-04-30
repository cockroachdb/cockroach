// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
import * as protos from "src/js/protos";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;
import TimeSeriesQueryAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
import TimeSeriesQueryDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;
import Long from "long";
import { History } from "history";
import { TimeWindow, TimeScale } from "src/redux/timewindow";
import { PayloadAction } from "src/interfaces/action";

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
  /**
   * Units are percentages expressed as fractional values of 1 (1.0 = 100%).
   */
  Percentage,
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

  // eslint-disable-next-line react/require-render-return
  render(): React.ReactElement<any> {
    throw new Error("Component <Axis /> should never render.");
  }
}

/**
 * MetricProps reperesents the properties of a Metric being selected as part of
 * a query.
 *
 * Note that there are redundant specifiers for several of the options
 * (derivatives, aggregators, downsamplers). These exist because, while the
 * exact specifiers (e.g. "aggregator") are convenient when constructing metrics
 * programmatically, the boolean specifiers (e.g. "aggregateMax") are convenient
 * when writing JSX directly. This is purely a syntactic helper.
 *
 * Only one option should be specified for each of the (derivative, aggregator,
 * downsampler); if multiple options are specified, the exact specifier takes
 * precedence.
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
  derivative?: TimeSeriesQueryDerivative;
  aggregator?: TimeSeriesQueryAggregator;
  downsampler?: TimeSeriesQueryAggregator;
}

/**
 * Metric is a React component which describes a Metric in a metrics query.
 *
 * This component should not be rendered directly; rather, a renderable
 * component should contain axes as children and use them only informationally
 * without rendering them.
 */
export class Metric extends React.Component<MetricProps> {
  // eslint-disable-next-line react/require-render-return
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
  setTimeRange?: (tw: TimeWindow) => PayloadAction<TimeWindow>;
  setTimeScale?: (ts: TimeScale) => PayloadAction<TimeScale>;
  history?: History;
}
