import * as React from "react";
import classNames = require("classnames");
import { ToolTip } from "./toolTip";

interface VisualizationProps {
  title: string;
  subtitle?: string;
  tooltip?: React.ReactNode;
  stale?: boolean;
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
      "visualization-wrapper": true,
      "viz-faded": stale || false,
    });

    let icon = stale ? "warning" : "info";

    return <div className={vizClasses}>
      <div className="viz-top">
        <div className="viz-info-icon">
          {
            // Display an icon if there is either a tooltip or if data is stale.
            (tooltip || stale) ? <div className={`icon-${icon}`} /> : ""
          }
        </div>
        {
          // Display tooltip if specified.
          (tooltip) ? <ToolTip text={tooltip} title={title} /> : ""
        }
      </div>
        { this.props.children }
      <div className="viz-bottom">
        <div className="viz-title">
          <div>{this.props.title}</div>
          { this.props.subtitle ? <div className="small">{this.props.subtitle}</div> : null }
        </div>
      </div>
    </div>;
  }
}
