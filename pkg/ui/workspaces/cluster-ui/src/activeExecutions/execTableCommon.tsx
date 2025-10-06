// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { Link } from "react-router-dom";

import { ColumnDescriptor } from "src/sortedtable";
import { capitalize, DATE_FORMAT, Duration } from "src/util";

import { Timestamp, Timezone } from "../timestamp";

import { StatusIcon } from "./statusIcon";
import { ExecutionType, ActiveExecution } from "./types";

export type ExecutionsColumn =
  | "applicationName"
  | "elapsedTime"
  | "execution"
  | "executionID"
  | "mostRecentStatement"
  | "retries"
  | "startTime"
  | "statementCount"
  | "status"
  | "timeSpentBlocking"
  | "timeSpentWaiting";

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
  mostRecentStatement: (type: ExecutionType) => {
    switch (type) {
      case "statement":
        return "Statement Execution";
      case "transaction":
      default:
        return "Most Recent Statement";
    }
  },
  retries: () => "Retries",
  startTime: () => "Start Time",
  statementCount: () => "Statements",
  status: () => "Status",
  timeSpentBlocking: () => "Time Spent Blocking",
  timeSpentWaiting: () => "Time Spent Waiting",
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
  mostRecentStatement: (execType: ExecutionType) => (
    <span>{getLabel("mostRecentStatement", execType)}</span>
  ),
  status: execType => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={
        <p>
          {`The status of the ${execType}'s execution. If
          "Preparing", the ${execType} is being parsed and planned.
          If "Executing", the ${execType} is currently being
          executed. If "Waiting", the ${execType} is currently
          experiencing contention. ${
            execType === "transaction"
              ? `If "Idle", the transaction is open but is not currently executing a statement.`
              : ``
          }`}
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
  timeSpentBlocking: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={
        <p>Amount of time this transaction has been experiencing contention.</p>
      }
    >
      {getLabel("timeSpentBlocking")}
    </Tooltip>
  ),
  timeSpentWaiting: () => (
    <Tooltip
      placement="bottom"
      style="tableTitle"
      content={<p>Amount of time this transaction has been blocked.</p>}
    >
      {getLabel("timeSpentWaiting")}
    </Tooltip>
  ),
};

function getID(item: ActiveExecution, execType: ExecutionType) {
  return execType === "transaction" ? item.transactionID : item.statementID;
}

function makeActiveExecutionColumns(
  execType: ExecutionType,
): Partial<Record<ExecutionsColumn, ColumnDescriptor<ActiveExecution>>> {
  return {
    executionID: {
      name: "executionID",
      title: executionsTableTitles.executionID(execType),
      cell: (item: ActiveExecution) => (
        <Link
          to={`/execution/${execType.toLowerCase()}/${getID(item, execType)}`}
        >
          {getID(item, execType)}
        </Link>
      ),
      sort: (item: ActiveExecution) => item.statementID,
      alwaysShow: true,
    },
    status: {
      name: "status",
      title: executionsTableTitles.status(execType),
      cell: (item: ActiveExecution) => (
        <span>
          <StatusIcon status={item.status} />
          {item.status}
        </span>
      ),
      sort: (item: ActiveExecution) => item.status,
    },
    startTime: {
      name: "startTime",
      title: (
        <>
          {executionsTableTitles.startTime(execType)} <Timezone />
        </>
      ),
      cell: (item: ActiveExecution) => (
        <Timestamp time={item.start} format={DATE_FORMAT} />
      ),
      sort: (item: ActiveExecution) => item.start.unix(),
    },
    elapsedTime: {
      name: "elapsedTime",
      title: executionsTableTitles.elapsedTime(execType),
      cell: (item: ActiveExecution) =>
        Duration(item.elapsedTime.asMilliseconds() * 1e6),
      sort: (item: ActiveExecution) => item.elapsedTime.asMilliseconds(),
    },
    timeSpentWaiting: {
      name: "timeSpentWaiting",
      title: executionsTableTitles.timeSpentWaiting(execType),
      cell: (item: ActiveExecution) =>
        Duration((item.timeSpentWaiting?.asMilliseconds() ?? 0) * 1e6),
      sort: (item: ActiveExecution) =>
        item.timeSpentWaiting?.asMilliseconds() || 0,
    },
    applicationName: {
      name: "applicationName",
      title: executionsTableTitles.applicationName(execType),
      cell: (item: ActiveExecution) => item.application,
      sort: (item: ActiveExecution) => item.application,
    },
  };
}

export const activeTransactionColumnsFromCommon =
  makeActiveExecutionColumns("transaction");

export const activeStatementColumnsFromCommon =
  makeActiveExecutionColumns("statement");
