// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import "./debugAnnotation.styl";

export interface DebugAnnotationProps {
  label: string;
  value: React.ReactNode;
}

/**
 * DebugAnnotation is an indicator showing a bit of information on the debug page.
 */
export default class DebugAnnotation extends React.Component<DebugAnnotationProps> {
  render() {
    return (
      <h3>
        <span className="debug-annotation__label">{this.props.label}:</span>{" "}
        <span className="debug-annotation__value">{this.props.value}</span>
      </h3>
    );
  }
}
