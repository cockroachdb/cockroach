// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { select, Selection } from "d3-selection";
import React, { useEffect, useRef } from "react";

export type Chart<T> = (sel: Selection<SVGElement, T, null, undefined>) => void;

/**
 * createChartComponent wraps a D3 reusable chart in a React component.
 * See https://bost.ocks.org/mike/chart/
 */
export default function createChartComponent<T>(
  containerTy: string,
  chart: Chart<T>,
) {
  return function WrappedChart(props: T) {
    const containerEl = useRef<SVGElement>(null);
    // Keep a ref to current props so the resize handler always has the latest.
    const propsRef = useRef(props);
    propsRef.current = props;

    // Redraw on every render — equivalent to shouldComponentUpdate calling
    // redraw(nextProps) and returning false.
    useEffect(() => {
      select(containerEl.current).datum(props).call(chart);
    });

    // Set up and tear down resize listener on mount/unmount.
    useEffect(() => {
      const handleResize = () => {
        select(containerEl.current).datum(propsRef.current).call(chart);
      };
      window.addEventListener("resize", handleResize);
      return () => window.removeEventListener("resize", handleResize);
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return React.createElement(containerTy, { ref: containerEl });
  };
}
