import * as React from "react";
import classNames = require("classnames");

interface ToolTipProps {
  text: React.ReactNode;
  title: string;
  position?: "right" | "left";
}

export class ToolTip extends React.Component<ToolTipProps, {}> {
  static defaultProps = {
    position: "right",
  };

  render() {
    let { position, title, text } = this.props;
    return <div className={ classNames("tooltip", "viz-tooltip", position) }>
      <div className="title">{ title }</div>
      <div className="content">{ text }</div>
    </div>;
  }
}
