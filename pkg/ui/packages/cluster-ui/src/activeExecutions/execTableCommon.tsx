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
      content={
        <p>The unique identifier for the execution of this {execType}.</p>
      }
    >
      {getLabel("executionID", execType)}
    </Tooltip>
  ),
  mostRecentStatement: () => <span>{getLabel("mostRecentStatement")}</span>,
  status: execType => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={
        <p>
          {`The status of the ${execType}'s execution. If
          "Preparing", the ${execType} is being parsed and planned.
          If "Executing", the ${execType} is currently being
          executed.`}
        </p>
      }
    >
      {getLabel("status")}
    </Tooltip>
  ),
  startTime: () => <span>{getLabel("startTime")}</span>,
  statementCount: () => <span>{getLabel("statementCount")}</span>,
  elapsedTime: () => <span>{getLabel("elapsedTime")}</span>,
  retries: () => <span>{getLabel("retries")}</span>,
};
