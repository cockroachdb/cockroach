import * as hljs from "highlight.js";
import React from "react";

import "./sqlhighlight.styl";

interface SqlBoxProps {
  value: string;
}

export class SqlBox extends React.Component<SqlBoxProps> {
  preNode: Node;

  shouldComponentUpdate(newProps: SqlBoxProps) {
    return newProps.value !== this.props.value;
  }

  componentDidMount() {
    hljs.highlightBlock(this.preNode);
  }

  componentDidUpdate() {
    hljs.highlightBlock(this.preNode);
  }

  render() {
    return (
      <pre className="sql-highlight" ref={ (node) => this.preNode = node }>
        { this.props.value }
      </pre>
    );
  }
}
