import React from "react";
import { Helmet } from "react-helmet";
import { Link } from "react-router";

/**
 * Renders the layout of the nodes page.
 */
export default class Layout extends React.Component<{}, {}> {
  render() {
    // TODO(mrtracy): this outer div is used to spare the children
    // `nav-container's styling. Should those styles apply only to `nav`?
    return <div>
      <Helmet>
        <title>Raft | Debug</title>
      </Helmet>
      <section className="section"><h1>Raft</h1></section>
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
