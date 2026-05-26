// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { DARK_BLUE, MAIN_BLUE } from "src/views/shared/colors";

interface LabelsProps {
  label: string;
  subLabel: string;
  tooltip?: string;
}

export class Labels extends React.Component<LabelsProps> {
  render() {
    return (
      <g
        fontFamily="Lato-Bold, Lato"
        fontSize="14"
        fontWeight="bold"
        letterSpacing="1"
        transform="translate(50 22)"
      >
        <title>{this.props.tooltip}</title>
        <text fill={DARK_BLUE}>{this.props.label}</text>
        <text fill={MAIN_BLUE} y="20">
          {this.props.subLabel}
        </text>
      </g>
    );
  }
}
