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
import { Tooltip } from "@cockroachlabs/ui-components";
import { InsightExecEnum } from "src/insights";

export const insightsColumnLabels = {
  executionID: "Execution ID",
  query: "Execution",
  insights: "Insights",
  startTime: "Start Time (UTC)",
  elapsedTime: "Elapsed Time",
  applicationName: "Application",
};

export type InsightsTableColumnKeys = keyof typeof insightsColumnLabels;

type InsightsTableTitleType = {
  [key in InsightsTableColumnKeys]: (execType: InsightExecEnum) => JSX.Element;
};

export function getLabel(
  key: InsightsTableColumnKeys,
  execType?: string,
): string {
  switch (execType) {
    case InsightExecEnum.TRANSACTION:
      return "Transaction " + insightsColumnLabels[key];
    case InsightExecEnum.STATEMENT:
      return "Statement " + insightsColumnLabels[key];
    default:
      return insightsColumnLabels[key];
  }
}

export const insightsTableTitles: InsightsTableTitleType = {
  executionID: (execType: InsightExecEnum) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            The {execType} execution ID. Click the {execType} execution ID to
            see more details.
          </p>
        }
      >
        {getLabel("executionID", execType)}
      </Tooltip>
    );
  },
  query: (execType: InsightExecEnum) => {
    let tooltipText = `The ${execType} query.`;
    if (execType == InsightExecEnum.TRANSACTION) {
      tooltipText = "The queries attempted in the transaction.";
    }
    return (
      <Tooltip style="tableTitle" placement="bottom" content={tooltipText}>
        {getLabel("query", execType)}
      </Tooltip>
    );
  },
  insights: (execType: InsightExecEnum) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            The category of insight identified for the {execType} execution.
          </p>
        }
      >
        {getLabel("insights")}
      </Tooltip>
    );
  },
  startTime: (execType: InsightExecEnum) => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={<p>The timestamp at which the {execType} started.</p>}
      >
        {getLabel("startTime")}
      </Tooltip>
    );
  },
  elapsedTime: (execType: InsightExecEnum) => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          <p>The time elapsed since the {execType} started execution.</p>
        }
      >
        {getLabel("elapsedTime")}
      </Tooltip>
    );
  },
  applicationName: (execType: InsightExecEnum) => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={<p>The name of the application that ran the {execType}.</p>}
      >
        {getLabel("applicationName")}
      </Tooltip>
    );
  },
};
