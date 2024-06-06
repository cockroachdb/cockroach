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
import { Tooltip } from "@cockroachlabs/ui-components";

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
