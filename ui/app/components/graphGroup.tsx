import * as React from "react";

import { MetricsDataProvider } from "../containers/metricsDataProvider";

/**
 * GraphGroup is a stateless react component that wraps a group of graphs (the
 * children of this component) in a MetricsDataProvider and some additional tags
 * relevant to the layout of our graphs pages.
 */
export default function(props: { groupId: string, childClassName?: string, children?: any }) {
  return <div>
  {
    React.Children.map(props.children, (child, idx) => {
      let key = props.groupId + idx.toString();
      return <div style={{float:"left"}} key={key} className={ props.childClassName || "" }>
        <MetricsDataProvider id={key}>
          { child }
        </MetricsDataProvider>
      </div>;
    })
  }
  </div>;
}
