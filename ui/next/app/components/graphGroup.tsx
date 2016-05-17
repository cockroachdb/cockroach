/// <reference path="../../typings/main.d.ts" />
import * as React from "react";
import * as d3 from "d3";

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { LineGraph, Axis, Metric } from "../components/linegraph";
import { Bytes } from "../util/format";
import { NanoToMilli } from "../util/convert";

/** 
 * GraphGroup is a stateless react component that wraps a group of graphs (the
 * children of this component) in a MetricsDataProvider and some additional tags
 * relevant to the layout of our graphs pages.
 */
export default function(props: { groupId: string, className?: string, children?: any }) {
  return <div>
  {
    React.Children.map(props.children, (child, idx) => {
      let key = props.groupId + idx.toString();
      return <div style={{float:"left"}} key={key} className={ props.className || "" }>
        <MetricsDataProvider id={key}>
          { child }
        </MetricsDataProvider>
      </div>;
    })
  }
  </div>;
}
