// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Moment } from "moment";
import { Filters } from "../queryFilter";

export enum InsightNameEnum {
  highWaitTime = "HIGH_WAIT_TIME",
}

export enum InsightExecEnum {
  TRANSACTION = "transaction",
  STATEMENT = "statement",
}

export type InsightEvent = {
  transactionID: string;
  fingerprintID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  elapsedTimeMillis: number;
  contentionThreshold: number;
  application: string;
  execType: InsightExecEnum;
};

export type InsightEventDetails = {
  executionID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  elapsedTime: number;
  contentionThreshold: number;
  application: string;
  fingerprintID: string;
  waitingExecutionID: string;
  waitingFingerprintID: string;
  waitingQueries: string[];
  contendedKey: string;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  execType: InsightExecEnum;
};

export type StatementInsightEvent = {
  // Some of these can be moved to a common InsightEvent type if txn query is updated.
  statementID: string;
  transactionID: string;
  statementFingerprintID: string;
  transactionFingerprintID: string;
  startTime: Moment;
  elapsedTimeMillis: number;
  sessionID: string;
  timeSpentWaiting?: moment.Duration;
  isFullScan: boolean;
  endTime: Moment;
  databaseName: string;
  rowsRead: number;
  rowsWritten: number;
  lastRetryReason?: string;
  priority: string;
  retries: number;
  problems: string[];
  query: string;
  application: string;
};

export type Insight = {
  name: InsightNameEnum;
  label: string;
  description: string;
  tooltipDescription: string;
};

export type EventExecution = {
  executionID: string;
  fingerprintID: string;
  queries: string[];
  startTime: Moment;
  elapsedTime: number;
  execType: InsightExecEnum;
};

const highWaitTimeInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
  latencyThreshold: number,
): Insight => {
  const description = `This ${execType} has been waiting for more than ${latencyThreshold}ms on other ${execType}s to execute.`;
  return {
    name: InsightNameEnum.highWaitTime,
    label: "High Wait Time",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const InsightTypes = [highWaitTimeInsight];

export const InsightExecOptions = [
  {
    value: InsightExecEnum.TRANSACTION.toString(),
    label: "Transaction Executions",
  },
  {
    value: InsightExecEnum.STATEMENT.toString(),
    label: "Statement Executions",
  },
];

export type InsightEventFilters = Omit<
  Filters,
  | "database"
  | "sqlType"
  | "fullScan"
  | "distributed"
  | "regions"
  | "nodes"
  | "username"
  | "sessionStatus"
  | "timeNumber"
  | "timeUnit"
>;

export type SchemaInsightEventFilters = Pick<
  Filters,
  "database" | "schemaInsightType"
>;

export type InsightType =
  | "DROP_INDEX"
  | "CREATE_INDEX"
  | "REPLACE_INDEX"
  | "HIGH_WAIT_TIME"
  | "HIGH_RETRIES"
  | "SUBOPTIMAL_PLAN"
  | "FAILED";

export interface InsightRecommendation {
  type: InsightType;
  database?: string;
  query?: string;
  indexDetails?: indexDetails;
  execution?: executionDetails;
  details?: insightDetails;
}

export interface indexDetails {
  table: string;
  indexID: number;
  indexName: string;
  lastUsed?: string;
}

export interface executionDetails {
  statement?: string;
  summary?: string;
  fingerprintID?: string;
  implicit?: boolean;
  retries?: number;
  indexRecommendations?: string[];
}

export interface insightDetails {
  duration: number;
  description: string;
}
