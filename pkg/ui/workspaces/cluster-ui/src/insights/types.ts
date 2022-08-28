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
  failedExecution = "FailedExecution",
  highContentionTime = "HighContentionTime",
  highRetryCount = "HighRetryCount",
  planRegression = "PlanRegression",
  suboptimalPlan = "SuboptimalPlan",
  unknown = "Unknown",
}

export enum InsightExecEnum {
  TRANSACTION = "transaction",
  STATEMENT = "statement",
}

export type TransactionInsightEvent = {
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

export type TransactionInsightEventDetails = {
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
  username: string;
  rowsRead: number;
  rowsWritten: number;
  lastRetryReason?: string;
  priority: string;
  retries: number;
  problems: string[];
  query: string;
  application: string;
  insights: Insight[];
  indexRecommendations: string[];
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

const highContentionTimeInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
  latencyThreshold: number,
): Insight => {
  const description = `This ${execType} has been waiting for more than ${latencyThreshold}ms on other ${execType}s to execute.`;
  return {
    name: InsightNameEnum.highContentionTime,
    label: "High Contention Time",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const unknownInsight = (execType: InsightExecEnum): Insight => {
  const description = `Unable to identify a specific cause for this ${execType}.`;
  return {
    name: InsightNameEnum.unknown,
    label: "Slow Execution",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const planRegressionInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} was slow because we picked the wrong plan, ` +
    `possibly due to outdated statistics, the statement using different literals or ` +
    `search conditions, or a change in the database schema.`;
  return {
    name: InsightNameEnum.planRegression,
    label: "Plan Regression",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const suboptimalPlanInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} was slow because a good plan was not available, whether ` +
    `due to outdated statistics or missing indexes.`;
  return {
    name: InsightNameEnum.suboptimalPlan,
    label: "Sub-Optimal Plan",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const highRetryCountInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} was slow because of being retried multiple times, again due ` +
    `to contention. The "high" threshold may be configured by the ` +
    `'sql.insights.high_retry_count.threshold' cluster setting.`;
  return {
    name: InsightNameEnum.highRetryCount,
    label: "High Retry Count",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const failedExecutionInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} execution failed completely, due to contention, resource ` +
    `saturation, or syntax errors.`;
  return {
    name: InsightNameEnum.failedExecution,
    label: "Failed Execution",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const InsightTypes = [highContentionTimeInsight];

export const getInsightFromProblem = (
  problem: string,
  execOption: InsightExecEnum,
  latencyThreshold?: number,
): Insight => {
  switch (problem) {
    case InsightNameEnum.highContentionTime:
      return highContentionTimeInsight(execOption, latencyThreshold);
    case InsightNameEnum.failedExecution:
      return failedExecutionInsight(execOption);
    case InsightNameEnum.planRegression:
      return planRegressionInsight(execOption);
    case InsightNameEnum.suboptimalPlan:
      return suboptimalPlanInsight(execOption);
    case InsightNameEnum.highRetryCount:
      return highRetryCountInsight(execOption);
    default:
      return unknownInsight(execOption);
  }
};

export const InsightExecOptions = new Map<string, string>([
  [InsightExecEnum.TRANSACTION.toString(), "Transaction Executions"],
  [InsightExecEnum.STATEMENT.toString(), "Statement Executions"],
]);

export type WorkloadInsightEventFilters = Pick<Filters, "app">;

export type SchemaInsightEventFilters = Pick<
  Filters,
  "database" | "schemaInsightType"
>;

export type InsightType =
  | "DropIndex"
  | "CreateIndex"
  | "ReplaceIndex"
  | "HighContentionTime"
  | "HighRetryCount"
  | "SuboptimalPlan"
  | "PlanRegression"
  | "FailedExecution"
  | "Unknown";

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
  duration?: number;
  description: string;
}
