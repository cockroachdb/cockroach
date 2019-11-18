// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as hljs from "highlight.js";
import React from "react";

import "./sqlhighlight.styl";

interface SqlBoxProps {
  value: string;
}

export class SqlBox extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();

  shouldComponentUpdate(newProps: SqlBoxProps) {
    return newProps.value !== this.props.value;
  }

  componentDidMount() {
    hljs.highlightBlock(this.preNode.current);
  }

  componentDidUpdate() {
    hljs.highlightBlock(this.preNode.current);
  }

  render() {
    return (
      <pre className="sql-highlight" ref={this.preNode}>
        { this.props.value }
      </pre>
    );
  }
}
