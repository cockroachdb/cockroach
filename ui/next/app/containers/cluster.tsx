/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

import { IndexListLink, ListLink } from "../components/listLink.tsx";

/**
 * ClusterMain renders the main content of the cluster page.
 */
export default class extends React.Component<{}, {}> {
  static title() {
    return <h2>Cluster</h2>;
  }

  render() {
    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <IndexListLink to="/cluster">Overview</IndexListLink>
          <ListLink to="/cluster/events">Events</ListLink>
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}
