// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Helmet } from "react-helmet";
import { NavLink } from "react-router-dom";

/**
 * Renders the layout of the nodes page.
 */
export default class Layout extends React.Component<{}, {}> {
  render() {
    // TODO(mrtracy): this outer div is used to spare the children
    // `nav-container's styling. Should those styles apply only to `nav`?
    return (
      <div>
        <Helmet title="Raft | Debug" />
        <section className="section">
          <h1 className="base-heading">Raft</h1>
        </section>
        <div className="nav-container">
          <ul className="nav">
            <li className="normal">
              <NavLink to="/raft/ranges" activeClassName="active">
                Ranges
              </NavLink>
            </li>
            <li className="normal">
              <NavLink to="/raft/messages/all" activeClassName="active">
                Messages
              </NavLink>
            </li>
          </ul>
        </div>
        {this.props.children}
      </div>
    );
  }
}
