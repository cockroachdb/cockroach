// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";

import { Anchor } from "src/anchor";
import { Timezone } from "src/timestamp";
import {
  contentionTime,
  planningExecutionTime,
  readFromDisk,
  readsAndWrites,
  statementDiagnostics,
  statementsRetries,
  statementsSql,
  writtenToDisk,
} from "src/util";

// Single place for column names. Used in table columns and in columns selector.
export const statisticsColumnLabels = {
  actions: "Actions",
  applicationName: "Application Name",
  bytesRead: "Bytes Read",
  clientAddress: "Client IP Address",
  contention: "Contention Time",
  cpu: "SQL CPU Time",
  database: "Database",
  diagnostics: "Diagnostics",
  executionCount: "Execution Count",
  lastExecTimestamp: "Last Execution Time",
  latencyMax: "Max Latency",
  latencyMin: "Min Latency",
  maxMemUsage: "Max Memory",
  maxMemUsed: "Maximum Memory Usage",
  memUsage: "Memory Usage",
  mostRecentStatement: "Most Recent Statement",
  networkBytes: "Network",
  numRetries: "Retries",
  numStatements: "Statements Run",
  regions: "Regions",
  regionNodes: "Regions/Nodes",
  retries: "Retries",
  rowsProcessed: "Rows Processed",
  sessionActiveDuration: "Session Active Duration",
  sessionDuration: "Session Duration",
  sessionStart: "Session Start Time",
  sessionTxnCount: "Transaction Count",
  statementFingerprintId: "Statement Fingerprint ID",
  statementStartTime: "Statement Start Time",
  statements: "Statements",
  statementsCount: "Statements",
  status: "Status",
  time: "Time",
  commitLatency: "Commit Latency",
  transactionFingerprintId: "Transaction Fingerprint ID",
  transactions: "Transactions",
  txnDuration: "Transaction Duration",
  username: "User Name",
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
        <>
          {getLabel("sessionStart")} <Timezone />
        </>
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
  sessionActiveDuration: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The amount of time the session has been actively running transactions."
        }
      >
        {getLabel("sessionActiveDuration")}
      </Tooltip>
    );
  },
  sessionTxnCount: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={"The number of transactions executed in this session."}
      >
        {getLabel("sessionTxnCount")}
      </Tooltip>
    );
  },
  status: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "A session is Active if it has an open explicit or implicit transaction (individual SQL statement) with a statement that is actively running or waiting to acquire a lock. A session is Idle if it is not executing a statement."
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
        <>
          {getLabel("statementStartTime")} <Timezone />
        </>
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
  applicationName: (statType?: StatisticType) => {
    let contentModifier = "session";
    switch (statType) {
      case "transaction":
        contentModifier = contentModifiers.transactions;
        break;
      case "statement":
        contentModifier = contentModifiers.statements;
        break;
    }
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={`The application that ran the ${contentModifier}.`}
      >
        {getLabel("applicationName")}
      </Tooltip>
    );
  },
  statementFingerprintId: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The statement fingerprint id is the combination of the statement fingerprint, the database it was executed on, the transaction type (implicit or explicit) and whether execution has failed (true or false)."
        }
      >
        {getLabel("statementFingerprintId")}
      </Tooltip>
    );
  },
  transactionFingerprintId: () => {
    return (
      <Tooltip
        style="tableTitle"
        placement="bottom"
        content={
          "The transaction fingerprint id represents the list of statement fingerprint ids in order of execution within that transaction."
        }
      >
        {getLabel("transactionFingerprintId")}
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
  transactions: () => {
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
              {`Cumulative number of executions of ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval.`}
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
  lastExecTimestamp: (statType: StatisticType) => {
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
          <p>Last time stamp on which the {contentModifier} was executed.</p>
        }
      >
        <>
          {getLabel("lastExecTimestamp")} <Timezone />
        </>
      </Tooltip>
    );
  },
  rowsProcessed: (statType: StatisticType) => {
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
              {"Average (mean) number of rows "}
              <Anchor href={readFromDisk} target="_blank">
                read
              </Anchor>
              {" from and "}
              <Anchor href={writtenToDisk} target="_blank">
                written
              </Anchor>
              {` to disk per execution for ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval.`}
            </p>
          </>
        }
      >
        {getLabel("rowsProcessed")}
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
              {` across all operators for ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval.`}
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
  commitLatency: (_statType: StatisticType) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <p>
            Average commit latency of this transaction. The gray bar indicates
            the mean latency. The blue bar indicates one standard deviation from
            the mean.
          </p>
        }
      >
        {getLabel("commitLatency")}
      </Tooltip>
    );
  },
  time: (statType: StatisticType) => {
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
              {"Average "}
              <Anchor href={planningExecutionTime} target="_blank">
                planning and execution time
              </Anchor>
              {` of ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval. `}
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
              {` with other ${contentModifier} within the specified time interval.`}
            </p>
            <p>
              The gray bar indicates mean contention time. The blue bar
              indicates one standard deviation from the mean. This time does not
              include the time it takes to stream results back to the client.
            </p>
          </>
        }
      >
        {getLabel("contention")}
      </Tooltip>
    );
  },
  cpu: (_: StatisticType) => {
    return (
      <Tooltip
        placement="bottom"
        style="tableTitle"
        content={
          <>
            <p>
              Average SQL CPU time spent executing within the specified time
              interval. It does not include SQL planning time nor KV execution
              time. The gray bar indicates mean SQL CPU time. The blue bar
              indicates one standard deviation from the mean.
            </p>
          </>
        }
      >
        {getLabel("cpu")}
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
              {`Maximum memory used by a ${contentModifier} with this fingerprint${fingerprintModifier} at any time
              during its execution within the specified time interval. `}
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
              {` (e.g., between regions and nodes) for ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval.`}
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
              {` (including internal and automatic retries) of ${contentModifier} with this fingerprint${fingerprintModifier} within the specified time interval.`}
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
            of all queries within the specified time interval.
          </p>
        }
      >
        {getLabel("workloadPct")}
      </Tooltip>
    );
  },
  regions: (statType: StatisticType) => {
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
        content={<p>Regions in which the {contentModifier} was executed.</p>}
      >
        {getLabel("regions")}
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
  diagnostics: () => {
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
  statementsCount: () => {
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
  latencyMax: (statType: StatisticType) => {
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
          <p>
            The highest latency value for all {contentModifier}
            executions with this fingerprint.
          </p>
        }
      >
        {getLabel("latencyMax")}
      </Tooltip>
    );
  },
  latencyMin: (statType: StatisticType) => {
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
          <p>
            The lowest latency value for all {contentModifier} executions with
            this fingerprint.
          </p>
        }
      >
        {getLabel("latencyMin")}
      </Tooltip>
    );
  },
};
