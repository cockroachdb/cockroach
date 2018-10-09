// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
