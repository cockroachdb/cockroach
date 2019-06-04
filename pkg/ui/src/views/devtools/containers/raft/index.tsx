// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
