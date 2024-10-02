// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "@cockroachlabs/ui-components";
import React, { ReactElement } from "react";

import { InsightExecEnum } from "src/insights/types";
import { Timezone } from "src/timestamp";

import { Anchor } from "../../../anchor";
import { contentModifiers } from "../../../statsTableUtil/statsTableUtil";
import { contentionTime, readFromDisk, writtenToDisk } from "../../../util";

export const insightsColumnLabels = {
  executionID: "Execution ID",
  latestExecutionID: "Latest Execution ID",
  waitingID: "Waiting Execution ID",
  waitingFingerprintID: "Waiting Fingerprint ID",
  query: "Execution",
  status: "Status",
  insights: "Insights",
  startTime: "Start Time",
  elapsedTime: "Elapsed Time",
  applicationName: "Application Name",
  username: "User Name",
  fingerprintID: "Fingerprint ID",
  numRetries: "Retries",
  isFullScan: "Full Scan",
  contention: "Contention Time",
  contentionStartTime: "Contention Start Time",
  rowsProcessed: "Rows Processed",
  schemaName: "Schema Name",
  databaseName: "Database Name",
  tableName: "Table Name",
  indexName: "Index Name",
  cpu: "SQL CPU Time",
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
      if (key === "latestExecutionID") {
        return "Latest Transaction Execution ID";
      }
      return "Transaction " + insightsColumnLabels[key];
    case InsightExecEnum.STATEMENT:
      if (key === "latestExecutionID") {
        return "Latest Statement Execution ID";
      }
      return "Statement " + insightsColumnLabels[key];
    default:
      return insightsColumnLabels[key];
  }
}

function makeToolTip(
  content: JSX.Element,
  columnKey: InsightsTableColumnKeys,
  execType?: InsightExecEnum,
  timezone?: JSX.Element,
): ReactElement {
  return (
    <Tooltip placement="bottom" style="tableTitle" content={content}>
      <>
        {getLabel(columnKey, execType)} {timezone}
      </>
    </Tooltip>
  );
}

export const insightsTableTitles: InsightsTableTitleType = {
  fingerprintID: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The {execType} fingerprint ID.</p>,
      "fingerprintID",
      execType,
    );
  },
  executionID: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The ID of the execution with the {execType} fingerprint.</p>,
      "executionID",
      execType,
    );
  },
  waitingFingerprintID: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The {execType} fingerprint ID.</p>,
      "waitingFingerprintID",
      execType,
    );
  },
  waitingID: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The ID of the waiting {execType}.</p>,
      "waitingID",
      execType,
    );
  },
  latestExecutionID: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>
        The execution ID of the latest execution with the {execType}{" "}
        fingerprint.
      </p>,
      "latestExecutionID",
      execType,
    );
  },
  query: (execType: InsightExecEnum) => {
    let tooltipText = `The ${execType} query.`;
    if (execType === InsightExecEnum.TRANSACTION) {
      tooltipText = "The queries attempted in the transaction.";
    }
    return makeToolTip(<p>{tooltipText}</p>, "query", execType);
  },
  status: (execType: InsightExecEnum) => {
    const tooltipText = `The ${execType} status`;
    return makeToolTip(<p>{tooltipText}</p>, "status");
  },
  insights: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The category of insight identified for the {execType} execution.</p>,
      "insights",
    );
  },
  startTime: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The timestamp at which the {execType} started.</p>,
      "startTime",
      undefined,
      <Timezone />,
    );
  },
  contentionStartTime: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The timestamp at which contention was detected for the {execType}.</p>,
      "contentionStartTime",
      undefined,
      <Timezone />,
    );
  },
  elapsedTime: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The time elapsed since the {execType} started execution.</p>,
      "elapsedTime",
    );
  },
  username: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The user that started the {execType}.</p>,
      "username",
    );
  },
  schemaName: (_execType: InsightExecEnum) => {
    return makeToolTip(<p>The name of the contended schema.</p>, "schemaName");
  },
  databaseName: (_execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The name of the contended database.</p>,
      "databaseName",
    );
  },
  tableName: (_execType: InsightExecEnum) => {
    return makeToolTip(<p>The name of the contended table.</p>, "tableName");
  },
  indexName: (_execType: InsightExecEnum) => {
    return makeToolTip(<p>The name of the contended index.</p>, "indexName");
  },
  applicationName: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The name of the application that ran the {execType}.</p>,
      "applicationName",
    );
  },
  numRetries: () => {
    return makeToolTip(
      <p>The number of times this statement encountered a retry.</p>,
      "numRetries",
    );
  },
  isFullScan: () => {
    return makeToolTip(
      <p>The statement executed a full scan.</p>,
      "isFullScan",
    );
  },
  contention: execType => {
    let contentModifier = "";
    switch (execType) {
      case InsightExecEnum.TRANSACTION:
        contentModifier = contentModifiers.transactions;
        break;
      case InsightExecEnum.STATEMENT:
        contentModifier = contentModifiers.statements;
        break;
    }
    return makeToolTip(
      <p>
        {`The time ${contentModifier} with this execution id was `}
        <Anchor href={contentionTime} target="_blank">
          in contention
        </Anchor>
        {` with other ${contentModifier} within the specified time interval.`}
      </p>,
      "contention",
    );
  },
  rowsProcessed: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>
        {"The number of rows "}
        <Anchor href={readFromDisk} target="_blank">
          read
        </Anchor>
        {" from and "}
        <Anchor href={writtenToDisk} target="_blank">
          written
        </Anchor>
        {` to disk per execution for ${execType} within the specified time interval.`}
      </p>,
      "rowsProcessed",
    );
  },
  cpu: (_: InsightExecEnum) => {
    return makeToolTip(
      <p>{`SQL CPU Time spent executing within the specified time interval. It
      does not include SQL planning time nor KV execution time.`}</p>,
      "cpu",
    );
  },
};
