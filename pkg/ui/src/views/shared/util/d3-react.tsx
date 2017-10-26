import d3 from "d3";
import React from "react";

type Chart<T> = (sel: d3.Selection<T>) => void;
type Charter<T> = () => Chart<T>;

/**
 * createChartComponent wraps a D3 reusable chart in a React component.
 * See https://bost.ocks.org/mike/chart/
 */
export default function createChartComponent<T>(chart: Charter<T>) {
  return class WrappedChart extends React.Component<T, {}> {
    svgEl: SVGElement;
    chart = chart();

    componentDidMount() {
      d3.select(this.svgEl)
        .datum(this.props)
        .call(this.chart);
    }

    shouldComponentUpdate(props: T) {
      d3.select(this.svgEl)
        .datum(props)
        .call(this.chart);

      return false;
    }

    render() {
      return <svg ref={(el) => this.svgEl = el} />;
    }
  };
}
