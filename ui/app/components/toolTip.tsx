import * as React from "react";
import classNames = require("classnames");

interface ToolTipProps {
  text: React.ReactNode;
  title: string;
  warningTitle?: string;
  warning?: React.ReactNode;
}

export class ToolTip extends React.Component<ToolTipProps, {}> {

  render() {
    let { title, text, warning, warningTitle } = this.props;
    return <div className={ classNames("tooltip", "viz-tooltip") }>
      <div className="title">{ title }</div>
      <div className="content">{ text }</div>
      {(warning || warningTitle) ?
        <div className="warning">
          <div className="icon-warning" />
          <div className="warning-title">{ warningTitle }</div>
          <div className="warningContent">{ warning }</div>
        </div>
        : null }
    </div>;
  }
}
