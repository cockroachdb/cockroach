import * as React from "react";

import { ListLink } from "../components/listLink";

/**
 * Renders the layout of the nodes page.
 */
export default class Layout extends React.Component<{}, {}> {
  static title() {
    return <h2>Raft</h2>;
  }

  render() {
    return <div className="nav-container">
      <ul className="nav">
        <ListLink to="/raft/ranges">Ranges</ListLink>
      </ul>
      { this.props.children }
    </div>;
  }
}
