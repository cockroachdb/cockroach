/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

import { ListLink } from "../components/listLink.tsx";

/**
 * Renders the layout of the nodes page.
 */
export default class Layout extends React.Component<{}, {}> {
  static title() {
    return <h2>Nodes</h2>;
  }

  render() {
    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <ListLink to="/nodes/overview">Overview</ListLink>
          <ListLink to="/nodes/graphs">Graphs</ListLink>
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}

