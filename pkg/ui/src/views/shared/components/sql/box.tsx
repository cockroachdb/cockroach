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
