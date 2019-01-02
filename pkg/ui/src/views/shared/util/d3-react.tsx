// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import d3 from "d3";
import React from "react";

type Chart<T> = (sel: d3.Selection<T>) => void;

/**
 * createChartComponent wraps a D3 reusable chart in a React component.
 * See https://bost.ocks.org/mike/chart/
 */
export default function createChartComponent<T>(containerTy: string, chart: Chart<T>) {
  return class WrappedChart extends React.Component<T> {
    containerEl: React.RefObject<Element> = React.createRef();

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
      d3.select(this.containerEl.current)
        .datum(props)
        .call(chart);
    }

    handleResize = () => {
      this.redraw();
    }

    addResizeHandler() {
      window.addEventListener("resize", this.handleResize);
    }

    removeResizeHandler() {
      window.removeEventListener("resize", this.handleResize);
    }

    render() {
      return React.createElement(
        containerTy,
        { ref: this.containerEl },
      );
    }
  };
}
