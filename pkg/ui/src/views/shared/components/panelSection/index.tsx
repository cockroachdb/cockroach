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

import "./panels.styl";

export class PanelSection extends React.Component {
  render() {
    return (
      <table className="panel-section">
        <tbody>
          { this.props.children }
        </tbody>
      </table>
    );
  }
}

export class PanelTitle extends React.Component {
  render() {
    return (
      <tr>
        <th colSpan={2} className="panel-title">{ this.props.children }</th>
      </tr>
    );
  }
}

export class PanelPair extends React.Component {
  render() {
    return (
      <tr className="panel-pair">
        { this.props.children }
      </tr>
    );
  }
}

export class Panel extends React.Component {
  render() {
    return (
      <td className="panel">
        { this.props.children }
      </td>
    );
  }
}
