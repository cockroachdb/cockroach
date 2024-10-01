// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import classNames from "classnames/bind";
import React from "react";

import { Insight } from "src/insights";

import styles from "./workloadInsights.module.scss";

const cx = classNames.bind(styles);

function mapInsightTypesToStatus(insight: Insight): string {
  switch (insight.label) {
    case "Failed Execution":
      return "danger";
    case "Slow Execution":
      return "info";
    default:
      return "warning";
  }
}

export function InsightCell(insight: Insight): React.ReactElement {
  const status = mapInsightTypesToStatus(insight);
  return (
    <Tooltip
      key={Math.random()}
      content={insight.tooltipDescription}
      style="tableTitle"
    >
      <span className={cx("insight-type", `insight-type--${status}`)}>
        {insight.label}
      </span>
    </Tooltip>
  );
}
