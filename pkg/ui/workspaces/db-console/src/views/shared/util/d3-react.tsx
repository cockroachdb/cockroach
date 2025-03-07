// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { select, Selection } from "d3-selection";
import React from "react";

export type Chart<T> = (sel: Selection<SVGElement, T, null, undefined>) => void;

/**
 * createChartComponent wraps a D3 reusable chart in a React component.
 * See https://bost.ocks.org/mike/chart/
 */
export default function createChartComponent<T>(
  containerTy: string,
  chart: Chart<T>,
) {
  return class WrappedChart extends React.Component<T> {
    containerEl: React.RefObject<SVGElement> = React.createRef();

    componentDidMount() {
      this.redraw();
      this.addResizeHandler();
    }

    componentWillUnmount() {
      this.removeResizeHandler();
    }

    shouldComponentUpdate(props: T) {
      this.redraw(props);

      return false;
    }

    redraw(props: T = this.props) {
      select(this.containerEl.current).datum(props).call(chart);
    }

    handleResize = () => {
      this.redraw();
    };

    addResizeHandler() {
      window.addEventListener("resize", this.handleResize);
    }

    removeResizeHandler() {
      window.removeEventListener("resize", this.handleResize);
    }

    render() {
      return React.createElement(containerTy, { ref: this.containerEl });
    }
  };
}
