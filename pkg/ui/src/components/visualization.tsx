// tslint:disable-next-line:no-var-requires
const spinner = require<string>("../../assets/spinner.gif");

import * as React from "react";
import classNames from "classnames";
import { ToolTipWrapper } from "./toolTip";

interface VisualizationProps {
  title: string;
  subtitle?: string;
  tooltip?: React.ReactNode;
  // If warning or warningTitle exist, they are appended to the tooltip
  // and the icon is changed to the warning icon.
  warningTitle?: string;
  warning?: React.ReactNode;
  // If stale is true, the visualization is faded
  // and the icon is changed to a warning icon.
  stale?: boolean;
  // If loading is true a spinner is shown instead of the graph.
  loading?: boolean;
}

/**
 * Visualization is a container for a variety of visual elements (such as
 * charts). It surrounds a visual element with some standard information, such
 * as a title and a tooltip icon.
 */
export default class extends React.Component<VisualizationProps, {}> {
  render() {
    let { title, tooltip, stale } = this.props;
    let vizClasses = classNames({
      "visualization": true,
      "visualization--faded": stale || false,
    });

    let tooltipNode: React.ReactNode = "";
    if (tooltip) {
      tooltipNode = (
        <div className="visualization__tooltip">
          <ToolTipWrapper text={tooltip}>
            <div className="visualization__tooltip-hover-area">
              <div className="visualization__info-icon">!</div>
            </div>
          </ToolTipWrapper>
        </div>
      );
    }

    return (
      <div className={vizClasses}>
        <div className="visualization__header">
          <div className="visualization__title">
            {title}
          </div>
          {
            this.props.subtitle ?
              <div className="visualization__subtitle">{this.props.subtitle}</div>
              : null
          }
          {
            tooltipNode
          }
        </div>
        <div className={"visualization__content" + (this.props.loading ? " visualization--loading" : "")}>
          {this.props.loading ? <img className="visualization__spinner" src={spinner} /> :  this.props.children }
        </div>
      </div>
    );
  }
}
