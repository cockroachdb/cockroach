import React from "react";
import { Link } from "react-router";

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
          <li className="normal">
            <Link to="/raft/ranges" activeClassName="active">Ranges</Link>
          </li>
          <li className="normal">
            <Link to="/raft/messages/all" activeClassName="active">Messages</Link>
          </li>
        </ul>
      </div>
      { this.props.children }
    </div>;
  }
}
