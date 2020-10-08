// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames";

import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import "./visualizations.styl";
import spinner from "assets/spinner.gif";

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
    const { title, tooltip, stale } = this.props;
    const vizClasses = classNames(
      "visualization",
      { "visualization--faded": stale || false },
    );
    const contentClasses = classNames(
      "visualization__content",
      { "visualization--loading": this.props.loading },
    );

    let tooltipNode: React.ReactNode = "";
    if (tooltip) {
      tooltipNode = (
        <div className="visualization__tooltip">
          <ToolTipWrapper text={tooltip}>
            <div className="visualization__tooltip-hover-area">
              <div className="visualization__info-icon">i</div>
            </div>
          </ToolTipWrapper>
        </div>
      );
    }

    return (
      <div className={vizClasses}>
        <div className="visualization__header">
          <span className="visualization__title">
            {title}
          </span>
          {
            this.props.subtitle ?
              <span className="visualization__subtitle">{this.props.subtitle}</span>
              : null
          }
          {
            tooltipNode
          }
        </div>
        <div className={contentClasses}>
          {this.props.loading ? <img className="visualization__spinner" src={spinner} /> :  this.props.children }
        </div>
      </div>
    );
  }
}
