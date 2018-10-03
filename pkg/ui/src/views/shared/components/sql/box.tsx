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
