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

// This enum corresponds to the string enum for `problems` in `cluster_execution_insights`
export enum InsightNameEnum {
  failedExecution = "FailedExecution",
  highContention = "HighContention",
  highRetryCount = "HighRetryCount",
  planRegression = "PlanRegression",
  suboptimalPlan = "SuboptimalPlan",
  slowExecution = "SlowExecution",
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
  contentionDuration: moment.Duration;
  contentionThreshold: number;
  application: string;
  execType: InsightExecEnum;
};

export type BlockedContentionDetails = {
  collectionTimeStamp: Moment;
  blockingExecutionID: string;
  blockingFingerprintID: string;
  blockingQueries: string[];
  contendedKey: string;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  contentionTimeMs: number;
};

export type TransactionInsightEventDetails = {
  executionID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  totalContentionTime: number;
  contentionThreshold: number;
  application: string;
  fingerprintID: string;
  blockingContentionDetails: BlockedContentionDetails[];
  execType: InsightExecEnum;
};

export type StatementInsightEvent = {
  // Some of these can be moved to a common InsightEvent type if txn query is updated.
  statementID: string;
  transactionID: string;
  statementFingerprintID: string;
  transactionFingerprintID: string;
  implicitTxn: boolean;
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
  causes: string[];
  problem: string;
  query: string;
  application: string;
  insights: Insight[];
  indexRecommendations: string[];
  planGist: string;
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
  contentionTimeMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  execType: InsightExecEnum;
};

const highContentionInsight = (
  execType: InsightExecEnum = InsightExecEnum.TRANSACTION,
  latencyThreshold?: number,
  contentionDuration?: number,
): Insight => {
  let waitDuration: string;
  if (
    latencyThreshold &&
    contentionDuration &&
    contentionDuration < latencyThreshold
  ) {
    waitDuration = `${contentionDuration}ms`;
  } else if (!latencyThreshold) {
    waitDuration =
      "longer than the value of the 'sql.insights.latency_threshold' cluster setting";
  } else {
    waitDuration = `longer than ${latencyThreshold}ms`;
  }
  const description = `This ${execType} waited on other ${execType}s to execute for ${waitDuration}.`;
  return {
    name: InsightNameEnum.highContention,
    label: "High Contention",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const slowExecutionInsight = (
  execType: InsightExecEnum,
  latencyThreshold: number,
): Insight => {
  let threshold = latencyThreshold + "ms";
  if (!latencyThreshold) {
    threshold =
      "the value of the 'sql.insights.latency_threshold' cluster setting";
  }
  const description = `This ${execType} took longer than ${threshold} to execute.`;
  return {
    name: InsightNameEnum.slowExecution,
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
    label: "Suboptimal Plan",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

const highRetryCountInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} has being retried more times than the value of the ` +
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

export const InsightTypes = [highContentionInsight]; // only used by getTransactionInsights to iterate over txn insights

export const getInsightFromProblem = (
  cause: string,
  execOption: InsightExecEnum,
  latencyThreshold?: number,
  contentionDuration?: number,
): Insight => {
  switch (cause) {
    case InsightNameEnum.highContention:
      return highContentionInsight(
        execOption,
        latencyThreshold,
        contentionDuration,
      );
    case InsightNameEnum.failedExecution:
      return failedExecutionInsight(execOption);
    case InsightNameEnum.planRegression:
      return planRegressionInsight(execOption);
    case InsightNameEnum.suboptimalPlan:
      return suboptimalPlanInsight(execOption);
    case InsightNameEnum.highRetryCount:
      return highRetryCountInsight(execOption);
    default:
      return slowExecutionInsight(execOption, latencyThreshold);
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
  | "AlterIndex"
  | "HighContention"
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
