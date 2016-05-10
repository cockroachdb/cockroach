import * as React from "react";

type TSResponse = cockroach.ts.TimeSeriesQueryResponseMessage;

/** 
 * SelectorProps reperesents the properties assigned to a selector component. A
 * selector describes a single time series that should be queried from the
 * server, along with some basic information on how that data should be rendered
 * in a graph context.
 */
export interface SelectorProps {
  name: string;
  sources?: string[];
  title?: string;
}

/**
 * Selector is a React component which describes a selector. This exists as a
 * component for convenient syntax, and should not be rendered directly; rather,
 * a renderable component will contain selectors, but will use them
 * only informationally withing rendering them.
 */
export class Selector extends React.Component<SelectorProps, {}> {
  render(): React.ReactElement<any> {
    throw new Error("Component <Selector /> should never render.");
  }
};

/**
 * AxisProps represents the properties of a renderable graph axis.
 */
interface AxisProps {
  title?: string;
}

/**
 * Axis is a React component which describes a renderable axis on a graph. This
 * exists as a component for convenient syntax, and should not be rendered
 * directly; rather, a renderable component will contain axes, but use them only
 * informationally without rendering them.
 */
export class Axis extends React.Component<AxisProps, {}> {
  render(): React.ReactElement<any> {
    throw new Error("Component <Axis /> should never render.");
  }
}

// MetricsDataComponentProps is an interface that should be implemented by any
// components directly contained by a MetricsDataProvider. It is used by a
// MetricsDataProvider to pass query data to its contained component.
export interface MetricsDataComponentProps {
  data?: TSResponse;
}

// TextGraph is a proof-of-concept component used to demonstrate that
// MetricsDataProvider is working correctly.
export class TextGraph extends React.Component<MetricsDataComponentProps, {}> {
  render() {
    return <div>{
      this.props.data && this.props.data.results ? this.props.data.results.join(":").toString() : ""
    }</div>;
  }
}
