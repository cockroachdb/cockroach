// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import spinner from "src/assets/spinner.gif";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";

import styles from "./visualizations.module.scss";
const cx = classNames.bind(styles);

interface VisualizationProps {
  title: string;
  subtitle?: string;
  tooltip?: React.ReactNode;
  // If stale is true, the visualization is faded
  // and the icon is changed to a warning icon.
  stale?: boolean;
  // If loading is true a spinner is shown instead of the graph.
  loading?: boolean;
  preCalcGraphSize?: boolean;
  children: React.ReactNode;
}

/**
 * Visualization is a container for a variety of visual elements (such as
 * charts). It surrounds a visual element with some standard information, such
 * as a title and a tooltip icon.
 */
export const Visualization: React.FC<VisualizationProps> = ({
  title,
  subtitle,
  tooltip,
  stale,
  loading,
  preCalcGraphSize,
  children,
}) => {
  const chartTitle: React.ReactNode = (
    <div>
      <span
        className={cx(
          "visualization-title",
          tooltip && "visualization-underline",
        )}
      >
        {title}
      </span>
      {subtitle && (
        <span className={cx("visualization-subtitle")}>{subtitle}</span>
      )}
    </div>
  );

  const tooltipNode: React.ReactNode = tooltip ? (
    <Tooltip placement="bottom" title={tooltip}>
      {chartTitle}
    </Tooltip>
  ) : (
    chartTitle
  );

  return (
    <div
      className={cx(
        "visualization",
        stale && "visualization-faded",
        preCalcGraphSize && "visualization-graph-sizing",
      )}
    >
      <div className={cx("visualization-header")}>{tooltipNode}</div>
      <div
        className={cx(
          "visualization-content",
          loading && "visualization-loading",
        )}
      >
        {loading ? (
          <img className={cx("visualization-spinner")} src={spinner} />
        ) : (
          children
        )}
      </div>
    </div>
  );
};
