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
import { capitalize } from "lodash";
import { Tooltip } from "@cockroachlabs/ui-components";

export type ExecutionsColumn =
  | "applicationName"
  | "elapsedTime"
  | "execution"
  | "executionID"
  | "mostRecentStatement"
  | "retries"
  | "startTime"
  | "statementCount"
  | "status";

export type ExecutionType = "statement" | "transaction";

export const executionsColumnLabels: Record<
  ExecutionsColumn,
  (execType: ExecutionType) => string
> = {
  applicationName: () => "Application",
  elapsedTime: () => "Elapsed Time",
  execution: (type: ExecutionType) => {
    return `${capitalize(type)} Execution`;
  },
  executionID: (type: ExecutionType) => {
    return `${capitalize(type)} Execution ID`;
  },
  mostRecentStatement: () => "Most Recent Statement",
  retries: () => "Retries",
  startTime: () => "Start Time (UTC)",
  statementCount: () => "Statements",
  status: () => "Status",
};

export type ExecutionsTableColumnKeys = keyof typeof executionsColumnLabels;

type ExecutionsTableTitleType = {
  [key in ExecutionsTableColumnKeys]: (execType: ExecutionType) => JSX.Element;
};

export function getLabel(
  key: ExecutionsTableColumnKeys,
  execType?: ExecutionType,
): string {
  return executionsColumnLabels[key](execType);
}

export const executionsTableTitles: ExecutionsTableTitleType = {
  applicationName: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>The name of the application.</p>}
    >
      {getLabel("applicationName")}
    </Tooltip>
  ),
  execution: (execType: ExecutionType) => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>The query being executed in this {execType}.</p>}
    >
      {getLabel("execution", execType)}
    </Tooltip>
  ),
  executionID: (execType: ExecutionType) => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>The id of the {execType} being executed.</p>}
    >
      {getLabel("executionID", execType)}
    </Tooltip>
  ),
  mostRecentStatement: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>The most recent statement executed in this transaction.</p>}
    >
      {getLabel("mostRecentStatement")}
    </Tooltip>
  ),
  status: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>{/* TODO (xzhang) */}</p>}
    >
      {getLabel("status")}
    </Tooltip>
  ),
  startTime: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>{/* TODO (xzhang)*/}</p>}
    >
      {getLabel("startTime")}
    </Tooltip>
  ),
  statementCount: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>{/* TODO (xzhang)*/}</p>}
    >
      {getLabel("statementCount")}
    </Tooltip>
  ),
  elapsedTime: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>{/* TODO (xzhang)*/}</p>}
    >
      {getLabel("elapsedTime")}
    </Tooltip>
  ),
  retries: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>{/* TODO (xzhang)*/}</p>}
    >
      {getLabel("retries")}
    </Tooltip>
  ),
};
