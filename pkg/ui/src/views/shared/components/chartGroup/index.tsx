import React from "react";

import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import "./chartGroup.styl";

interface ChartGroupProps {
  title: string;
  tooltip?: React.ReactNode;
}

export class ChartGroup extends React.Component<ChartGroupProps> {
  render() {
    let tooltipNode: React.ReactNode = null;
    if (this.props.tooltip) {
      tooltipNode = (
        <div className="chart-group__tooltip">
          <ToolTipWrapper text={this.props.tooltip}>
            <div className="chart-group__tooltip-hover-area">
              <div className="chart-group__info-icon">i</div>
            </div>
          </ToolTipWrapper>
        </div>
      );
    }

    return (
      <div className="chart-group">
        <div className="chart-group__header">
          <span className="chart-group__title">
            { this.props.title }
          </span>
          { tooltipNode }
        </div>
        <div className="chart-group__content">
          { this.props.children }
        </div>
      </div>
    );
  }
}
