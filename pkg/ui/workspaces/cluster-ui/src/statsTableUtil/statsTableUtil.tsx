// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Anchor } from "src/anchor";
import moment from "moment";

import { Tooltip } from "@cockroachlabs/ui-components";
import {
  statementDiagnostics,
  statementsRetries,
  statementsSql,
  statementsTimeInterval,
  readFromDisk,
  writtenToDisk,
  planningExecutionTime,
  contentionTime,
  readsAndWrites,
} from "src/util";
import { AggregateStatistics } from "src/statementsTable";

export type NodeNames = { [nodeId: string]: string };

// Single place for column names. Used in table columns and in columns selector.
export const statisticsColumnLabels = {
  sessionStart: "Session Start Time (UTC)",
  sessionDuration: "Session Duration",
  mostRecentStatement: "Most Recent Statement",
  status: "Status",
  statementStartTime: "Statement Start Time (UTC)",
  txnDuration: "Transaction Duration",
  actions: "Actions",
  memUsage: "Memory Usage",
  maxMemUsed: "Maximum Memory Usage",
  numRetries: "Retries",
  numStatements: "Statements Run",
  clientAddress: "Client IP Address",
  username: "User Name",
  applicationName: "Application Name",
  bytesRead: "Bytes Read",
  contention: "Contention",
  database: "Database",
  diagnostics: "Diagnostics",
  executionCount: "Execution Count",
  maxMemUsage: "Max Memory",
  networkBytes: "Network",
  regionNodes: "Regions/Nodes",
  retries: "Retries",
  rowsRead: "Rows Read",
  rowsWritten: "Rows Written",
  statements: "Statements",
  statementsCount: "Statements",
  time: "Time",
  transactions: "Transactions",
  workloadPct: "% of All Runtime",
};

export const contentModifiers = {
  transactionCapital: "Transaction",
  transaction: "transaction",
  transactions: "transactions",
  transactionFingerprint: "transaction fingerprint",
  statementCapital: "Statement",
  statement: "statement",
  statements: "statements",
};

export type StatisticType =
  | "statement"
  | "session"
  | "transaction"
  | "transactionDetails";
export type StatisticTableColumnKeys = keyof typeof statisticsColumnLabels;

type StatisticTableTitleType = {
  [key in StatisticTableColumnKeys]: (statType: StatisticType) => JSX.Element;
};

export function getLabel(
  key: StatisticTableColumnKeys,
  statType?: StatisticType,
): string {
  if (key !== "time") return statisticsColumnLabels[key];

  switch (statType) {
    case "transaction":
      return (
        contentModifiers.transactionCapital + " " + statisticsColumnLabels[key]
      );
    case "statement":
      return (
        contentModifiers.statementCapital + " " + statisticsColumnLabels[key]
      );
    case "transactionDetails":
      return (
        contentModifiers.statementCapital + " " + statisticsColumnLabels[key]
      );
    default:
      return statisticsColumnLabels[key];
  }
}

// statisticsTableTitles is a mapping between statistic table columns and functions.
// Each function provides tooltip information for its respective statistic column.
// The function parameter is a StatisticType, which represents the type
// of data the statistics are based on (e.g. statements, transactions, or transactionDetails). The
// StatisticType is used to modify the content of the tooltip.
export const statisticsTableTitles: StatisticTableTitleType = {
  sessionStart: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The timestamp at which the session started."}
      >
        {getLabel("sessionStart")}
      </Tooltip>
    );
  },
  sessionDuration: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The amount of time the session has been open."}
      >
        {getLabel("sessionDuration")}
      </Tooltip>
    );
  },
  status: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "A session is active if it has an open transaction (including implicit transactions, which are individual SQL statements), and idle if it has no open transaction."
        }
      >
        {getLabel("status")}
      </Tooltip>
    );
  },
  mostRecentStatement: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "If more than one statement is active, the most recent statement is shown. If the session is idle, the last statement is shown."
        }
      >
        {getLabel("mostRecentStatement")}
      </Tooltip>
    );
  },
  statementStartTime: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The timestamp at which the statement started."}
      >
        {getLabel("statementStartTime")}
      </Tooltip>
    );
  },
  memUsage: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "Amount of memory currently allocated to this session, followed by the maximum amount of memory this session has ever been allocated."
        }
      >
        {getLabel("memUsage")}
      </Tooltip>
    );
  },
  clientAddress: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The IP address/port of the client that opened the session."}
      >
        {getLabel("clientAddress")}
      </Tooltip>
    );
  },
  username: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The user that opened the session."}
      >
        {getLabel("username")}
      </Tooltip>
    );
  },
  applicationName: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The application that ran the session."}
      >
        {getLabel("applicationName")}
      </Tooltip>
    );
  },
  actions: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"Actions to take on the session."}
      >
        {getLabel("actions")}
      </Tooltip>
    );
  },
  maxMemUsed: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The maximum amount of allocated memory this session ever had."
        }
      >
        {getLabel("maxMemUsage")}
      </Tooltip>
    );
  },
  numRetries: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The number of times this transaction encountered a retry."}
      >
        {getLabel("retries")}
      </Tooltip>
    );
  },
  numStatements: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "Number of statements that have been run in this transaction so far."
        }
      >
        {getLabel("numStatements")}
      </Tooltip>
    );
  },
  txnDuration: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The duration of the open transaction, if there is one."}
      >
        {getLabel("txnDuration")}
      </Tooltip>
    );
  },
  statements: () => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"A "}
              <Anchor href={statementsSql} target="_blank">
                statement fingerprint
              </Anchor>
            </p>
            <p>
              {` represents one or more SQL statements by replacing the literal values (e.g., numbers and strings) with 
              underscores (_). To view additional details of a SQL statement fingerprint, click the fingerprint to 
              open the Statement Details page.`}
            </p>
          </>
        }
      >
        {getLabel("statements")}
      </Tooltip>
    );
  },
  transactions: (statType: StatisticType) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {`A transaction fingerprint represents one or more SQL transactions by replacing the literal values (e.g., numbers and strings) with 
              underscores (_). To view additional details of a SQL transaction fingerprint, click the fingerprint to 
              open the Transaction Details page.`}
            </p>
          </>
        }
      >
        {getLabel("transactions")}
      </Tooltip>
    );
  },
  executionCount: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {`Cumulative number of executions of ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              {"The bar indicates the ratio of runtime success (gray) to "}
              <Anchor href={statementsRetries} target="_blank">
                retries
              </Anchor>
              {" (red) for the SQL statement fingerprint."}
            </p>
          </>
        }
      >
        {getLabel("executionCount")}
      </Tooltip>
    );
  },
  database: (statType: StatisticType) => {
    let contentModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transaction;
        break;
      case "statement":
        contentModifier = contentModifiers.statement;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={<p>Database on which the {contentModifier} was executed.</p>}
      >
        {getLabel("database")}
      </Tooltip>
    );
  },
  rowsRead: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Aggregation of all rows "}
              <Anchor href={readFromDisk} target="_blank">
                read from disk
              </Anchor>
              {` across all operators for ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              The gray bar indicates the mean number of rows read from disk. The
              blue bar indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("rowsRead")}
      </Tooltip>
    );
  },
  bytesRead: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Aggregation of all bytes "}
              <Anchor href={readFromDisk} target="_blank">
                read from disk
              </Anchor>
              {` across all operators for ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              The gray bar indicates the mean number of bytes read from disk.
              The blue bar indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("bytesRead")}
      </Tooltip>
    );
  },
  rowsWritten: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transaction;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Aggregation of all rows "}
              <Anchor href={writtenToDisk} target="_blank">
                written to disk
              </Anchor>
              {` across all operators for ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              The gray bar indicates the mean number of rows written to disk.
              The blue bar indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("rowsWritten")}
      </Tooltip>
    );
  },
  time: (statType: StatisticType) => {
    let columnLabel = "";
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        columnLabel = contentModifiers.transactionCapital;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        columnLabel = contentModifiers.statementCapital;
        break;
      case "transactionDetails":
        columnLabel = contentModifiers.statementCapital;
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Average "}
              <Anchor href={planningExecutionTime} target="_blank">
                planning and execution time
              </Anchor>
              {` of ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified time interval. `}
            </p>
            <p>
              The gray bar indicates the mean latency. The blue bar indicates
              one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("time", statType)}
      </Tooltip>
    );
  },
  contention: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {`Average time ${contentModifier} with this fingerprint${fingerprintModifier} were `}
              <Anchor href={contentionTime} target="_blank">
                in contention
              </Anchor>
              {` with other ${contentModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              The gray bar indicates mean contention time. The blue bar
              indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("contention")}
      </Tooltip>
    );
  },
  maxMemUsage: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transaction;
        break;
      case "statement":
        contentModifier = contentModifiers.statement;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statement;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {`Maximum memory used by a ${contentModifier} with this fingerprint${fingerprintModifier} at any time during its execution within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              The gray bar indicates the average max memory usage. The blue bar
              indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("maxMemUsage")}
      </Tooltip>
    );
  },
  networkBytes: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Amount of "}
              <Anchor href={readsAndWrites} target="_blank">
                data transferred over the network
              </Anchor>
              {` (e.g., between regions and nodes) for ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .&nbsp;
            </p>
            <p>
              If this value is 0, the statement was executed on a single node.
            </p>
            <p>
              The gray bar indicates the mean number of bytes sent over the
              network. The blue bar indicates one standard deviation from the
              mean.
            </p>
          </>
        }
      >
        {getLabel("networkBytes")}
      </Tooltip>
    );
  },
  retries: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Cumulative number of "}
              <Anchor href={statementsRetries} target="_blank">
                retries
              </Anchor>
              {` of ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified time interval.`}
            </p>
          </>
        }
      >
        {getLabel("retries")}
      </Tooltip>
    );
  },
  workloadPct: (statType: StatisticType) => {
    let contentModifier = "";
    let fingerprintModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
      case "transactionDetails":
        contentModifier = contentModifiers.statements;
        fingerprintModifier =
          " for this " + contentModifiers.transactionFingerprint;
        break;
    }
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            % of runtime all {contentModifier} with this fingerprint
            {fingerprintModifier} represent, compared to the cumulative runtime
            of all queries within the last hour or specified time interval.
          </p>
        }
      >
        {getLabel("workloadPct")}
      </Tooltip>
    );
  },
  regionNodes: (statType: StatisticType) => {
    let contentModifier = "";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transaction;
        break;
      case "statement":
        contentModifier = contentModifiers.statement;
        break;
    }

    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>Regions/Nodes in which the {contentModifier} was executed.</p>
        }
      >
        {getLabel("regionNodes")}
      </Tooltip>
    );
  },
  diagnostics: (statType: StatisticType) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {"Option to activate "}
              <Anchor href={statementDiagnostics} target="_blank">
                diagnostics
              </Anchor>
              {
                " for each statement. If activated, this displays the status of diagnostics collection ("
              }
              <code>WAITING</code>, <code>READY</code>, OR <code>ERROR</code>).
            </p>
          </>
        }
      >
        {getLabel("diagnostics")}
      </Tooltip>
    );
  },
  statementsCount: (statType: StatisticType) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              {`The number of statements being executed on this transaction fingerprint`}
            </p>
          </>
        }
      >
        {getLabel("statementsCount")}
      </Tooltip>
    );
  },
};

export function formatAggregationIntervalColumn(
  aggregatedTs: number,
  interval: number,
): string {
  const formatStr = "MMM D, h:mm A";
  const formatStrWithoutDay = "h:mm A";
  const start = moment.unix(aggregatedTs).utc();
  const end = moment.unix(aggregatedTs + interval).utc();
  const isSameDay = start.isSame(end, "day");

  return `${start.format(formatStr)} - ${end.format(
    isSameDay ? formatStrWithoutDay : formatStr,
  )}`;
}
