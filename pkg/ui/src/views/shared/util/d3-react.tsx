import d3 from "d3";
import React from "react";

type Chart<T> = (sel: d3.Selection<T>) => void;

/**
 * createChartComponent wraps a D3 reusable chart in a React component.
 * See https://bost.ocks.org/mike/chart/
 */
export default function createChartComponent<T>(containerTy: string, chart: Chart<T>) {
  return class WrappedChart extends React.Component<T> {
    containerEl: Element;

    componentDidMount() {
      d3.select(this.containerEl)
        .datum(this.props)
        .call(chart);
    }

    shouldComponentUpdate(props: T) {
      d3.select(this.containerEl)
        .datum(props)
        .call(chart);

      return false;
    }

    render() {
      return React.createElement(
        containerTy,
        { ref: (el) => this.containerEl = el },
      );
    }
  };
}
