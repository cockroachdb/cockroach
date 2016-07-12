import * as React from "react";
import classNames = require("classnames");

interface ToolTipProps {
  text: React.ReactNode;
  title: string;
  warningTitle?: string;
  warning?: React.ReactNode;
  position?: "right" | "left";
}

export class ToolTip extends React.Component<ToolTipProps, {}> {
  static defaultProps = {
    position: "right",
  };

  render() {
    let { position, title, text, warning, warningTitle } = this.props;
    return <div className={ classNames("tooltip", "viz-tooltip", position) }>
      <div className="title">{ title }</div>
      <div className="content">{ text }</div>
      {warning ?
        <div className="warning">
          <div className="icon-warning" />
          <div className="warning-title">{ warningTitle }</div>
          <div className="warningContent">{ warning }</div>
        </div>
        : null }
    </div>;
  }
}
