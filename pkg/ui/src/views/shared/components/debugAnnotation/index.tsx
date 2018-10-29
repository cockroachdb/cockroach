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

import "./debugAnnotation.styl";

export interface DebugAnnotationProps {
  label: string;
  value: string;
}

/**
 * DebugAnnotation is an indicator showing a bit of information on the debug page.
 */
export default class DebugAnnotation extends React.Component<DebugAnnotationProps> {
  render() {
    return (
      <h3>
        <span className="debug-annotation__label">{ this.props.label }:</span>
        {" "}
        <span className="debug-annotation__value">{ this.props.value }</span>
      </h3>
    );
  }
}
