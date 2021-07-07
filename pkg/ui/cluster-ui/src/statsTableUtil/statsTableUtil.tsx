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

import { Tooltip } from "@cockroachlabs/ui-components";
import {
  statementDiagnostics,
  statementsRetries,
  statementsSql,
  statementsTimeInterval,
  readFromDisk,
  planningExecutionTime,
  contentionTime,
  readsAndWrites,
} from "src/util";

export type NodeNames = { [nodeId: string]: string };

// Single place for column names. Used in table columns and in columns selector.
export const statisticsColumnLabels = {
  statements: "Statements",
  database: "Database",
  executionCount: "Execution Count",
  rowsRead: "Rows Read",
  bytesRead: "Bytes Read",
  time: "Time",
  contention: "Contention",
  maxMemUsage: "Max Memory",
  networkBytes: "Network",
  retries: "Retries",
  workloadPct: "% of All Runtime",
  regionNodes: "Regions/Nodes",
  diagnostics: "Diagnostics",
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

export type StatisticType = "statement" | "transaction" | "transactionDetails";
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
  statements: (statType: StatisticType) => {
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
              .
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
        content={<p>Database on which the ${contentModifier} was executed.</p>}
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
              .
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
              .
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
              {` of ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified time interval.`}
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
              .
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
              .
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
              {"Amount of data "}
              <Anchor href={readsAndWrites} target="_blank">
                data transferred over the network
              </Anchor>
              {` (e.g., between regions and nodes) for ${contentModifier} with this fingerprint${fingerprintModifier} within the last hour or specified `}
              <Anchor href={statementsTimeInterval} target="_blank">
                time interval
              </Anchor>
              .
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
            % of runtime all ${contentModifier} with this fingerprint$
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
          <p>Regions/Nodes in which the ${contentModifier} was executed.</p>
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
};
