/// <reference path="../../typings/index.d.ts" />
import * as React from "react";

import { IndexListLink, ListLink } from "../components/listLink.tsx";
import TimeScaleSelector from "./timescale";

/**
 * ClusterMain renders the main content of the cluster page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Cluster</h2>;
  }

  render() {
    // Determine whether or not the time scale options should be displayed.
    let child = React.Children.only(this.props.children);
    let displayTimescale = (child as any).type.displayTimeScale === true;

    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <IndexListLink to="/cluster">Overview</IndexListLink>
          <ListLink to="/cluster/events">Events</ListLink>
          { displayTimescale ? <TimeScaleSelector/> : null }
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}
