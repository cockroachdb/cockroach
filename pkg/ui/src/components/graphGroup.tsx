import * as React from "react";

import { MetricsDataProvider } from "../containers/metricsDataProvider";

/**
 * GraphGroup is a stateless react component that wraps a group of graphs (the
 * children of this component) in a MetricsDataProvider and some additional tags
 * relevant to the layout of our graphs pages.
 */
export default function (props: { groupId: string, hide: boolean, childClassName?: string, children?: React.ReactNode }) {
  if (props.hide) {
    return null;
  }

  return <div>
  {
    React.Children.map(props.children, (child, idx) => {
      let key = props.groupId + idx.toString();
      // Special case h2 tags which are used as the graph group title.
      if ((child as React.ReactElement<any>).type === "h2") {
        return <div>{ child }</div>;
      }
      return <div key={key} className={ props.childClassName || "" }>
        <MetricsDataProvider id={key}>
          { child }
        </MetricsDataProvider>
      </div>;
    })
  }
  </div>;
}
