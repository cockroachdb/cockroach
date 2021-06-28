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

import "./panels.styl";

export class PanelSection extends React.Component {
  render() {
    return (
      <table className="panel-section">
        <tbody>{this.props.children}</tbody>
      </table>
    );
  }
}

export class PanelTitle extends React.Component {
  render() {
    return (
      <tr>
        <th colSpan={2} className="panel-title">
          {this.props.children}
        </th>
      </tr>
    );
  }
}

export class PanelPair extends React.Component {
  render() {
    return <tr className="panel-pair">{this.props.children}</tr>;
  }
}

export class Panel extends React.Component {
  render() {
    return <td className="panel">{this.props.children}</td>;
  }
}
