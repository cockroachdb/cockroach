/// <reference path="../../typings/main.d.ts" />
import * as React from "react";

import { ListLink } from "../components/listLink.tsx";

/**
 * Renders the layout of the nodes page.
 */
export default class Layout extends React.Component<{}, {}> {
  static title() {
    return <h2>Raft</h2>;
  }

  render() {
    // TODO: The first div seems superfluous, remove after switch to ui/next.
    return <div>
      <div className="nav-container">
        <ul className="nav">
          <ListLink to="/raft/ranges">Ranges</ListLink>
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}

