import * as React from "react";

import { IndexListLink, ListLink } from "../components/listLink";
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
    //
    // TODO(mrtracy): is this the right object heirarchy? Seems odd to inspect
    // the child. Also see about removing the `any` below.
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
