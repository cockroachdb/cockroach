// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { ReactElement } from "react";
import { Tooltip } from "@cockroachlabs/ui-components";
import { InsightExecEnum } from "src/insights";

export const insightsColumnLabels = {
  executionID: "Execution ID",
  query: "Execution",
  insights: "Insights",
  startTime: "Start Time (UTC)",
  elapsedTime: "Elapsed Time",
  applicationName: "Application Name",
  username: "User Name",
  fingerprintID: "Fingerprint ID",
  numRetries: "Retries",
  isFullScan: "Full Scan",
  contention: "Contention",
  rowsRead: "Rows Read",
  rowsWritten: "Rows Written",
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

function makeToolTip(
  content: JSX.Element,
  columnKey: InsightsTableColumnKeys,
  execType?: InsightExecEnum,
): ReactElement {
  return (
    <Tooltip placement="bottom" style="tableTitle" content={content}>
      {getLabel(columnKey, execType)}
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
      <p>
        The execution ID of the latest execution with the {execType}{" "}
        fingerprint.
      </p>,
      "executionID",
      execType,
    );
  },
  query: (execType: InsightExecEnum) => {
    let tooltipText = `The ${execType} query.`;
    if (execType == InsightExecEnum.TRANSACTION) {
      tooltipText = "The queries attempted in the transaction.";
    }
    return makeToolTip(<p>tooltipText</p>, "query", execType);
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
    );
  },
  elapsedTime: (execType: InsightExecEnum) => {
    return makeToolTip(
      <p>The time elapsed since the {execType} started execution.</p>,
      "elapsedTime",
    );
  },
  username: (execType: InsightExecEnum) => {
    return makeToolTip(<p>The user that opened the {execType}.</p>, "username");
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
      <p>
        The table is scanned on all key ranges of the rides_pkey index (i.e., a
        full table scan).
      </p>,
      "isFullScan",
    );
  },
  contention: () => {
    return makeToolTip(
      <p>
        Lock contention happens when multiple processes are trying to access the
        same data at the same time. In the context of a SQL database, this might
        mean that multiple transactions are trying to update the same row at the
        same time, for example.
      </p>,
      "contention",
    );
  },
  rowsRead: () => {
    return makeToolTip(
      <p>
        The number of rows read to disk per execution for statements with this
        fingerprint within the specified time interval.
      </p>,
      "rowsRead",
    );
  },
  rowsWritten: () => {
    return makeToolTip(
      <p>
        The number of rows written to disk per execution for statements with
        this fingerprint within the specified time interval.
      </p>,
      "rowsWritten",
    );
  },
};
