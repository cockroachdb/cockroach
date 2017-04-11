import * as React from "react";

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
}
