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
    // TODO(mrtracy): this outer div is used to spare the children
    // `nav-container's styling. Should those styles apply only to `nav`?
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
