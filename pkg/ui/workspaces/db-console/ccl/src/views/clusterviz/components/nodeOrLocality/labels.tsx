// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
