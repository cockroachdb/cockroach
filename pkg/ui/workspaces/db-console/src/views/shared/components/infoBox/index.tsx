// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import "./infoBox.styl";

export default class InfoBox extends React.Component {
  render() {
    return <div className="info-box">{this.props.children}</div>;
  }
}
