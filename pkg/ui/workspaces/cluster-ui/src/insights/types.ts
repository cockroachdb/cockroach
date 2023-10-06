// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Moment } from "moment-timezone";
import { Filters } from "../queryFilter";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { InsightCause } from "../api";

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

export enum StatementStatus {
  COMPLETED = "Completed",
  FAILED = "Failed",
}

export enum TransactionStatus {
  COMPLETED = "Completed",
  FAILED = "Failed",
  // Unimplemented, see https://github.com/cockroachdb/cockroach/issues/98219/.
  CANCELLED = "Cancelled",
}

// Common fields for both txn and stmt insights.
export type InsightEventBase = {
  application: string;
  contentionTime?: moment.Duration;
  cpuSQLNanos: number;
  elapsedTimeMillis: number;
  endTime: Moment;
  implicitTxn: boolean;
  insights: Insight[];
  lastRetryReason?: string;
  priority: string;
  query: string;
  retries: number;
  rowsRead: number;
  rowsWritten: number;
  sessionID: string;
  startTime: Moment;
  transactionExecutionID: string;
  transactionFingerprintID: string;
  username: string;
  errorCode: string;
  errorMsg: string;
};

export type TxnInsightEvent = InsightEventBase & {
  status: TransactionStatus;
  stmtExecutionIDs: string[];
};

// Information about the blocking transaction and schema.
export type ContentionDetails = {
  collectionTimeStamp: Moment;
  blockingExecutionID: string;
  blockingTxnFingerprintID: string;
  blockingTxnQuery: string[];
  waitingTxnID: string;
  waitingTxnFingerprintID: string;
  waitingStmtID: string;
  waitingStmtFingerprintID: string;
  contendedKey: string;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  contentionTimeMs: number;
};

export type TxnContentionInsightDetails = {
  transactionExecutionID: string;
  application: string;
  transactionFingerprintID: string;
  blockingContentionDetails: ContentionDetails[];
  execType: InsightExecEnum;
  insightName: string;
};

export type TxnInsightDetails = {
  // Querying from virtual tables is expensive.
  // This data is segmented into 3 parts so that we can
  // selective fetch missing info on the details page.
  txnDetails?: TxnInsightEvent;
  blockingContentionDetails?: ContentionDetails[];
  statements?: StmtInsightEvent[];
  execType?: InsightExecEnum;
};

// Shown on the stmt insights overview page.
export type StmtInsightEvent = InsightEventBase & {
  statementExecutionID: string;
  statementFingerprintID: string;
  isFullScan: boolean;
  contentionEvents?: ContentionDetails[];
  indexRecommendations: string[];
  planGist: string;
  databaseName: string;
  execType?: InsightExecEnum;
  status: StatementStatus;
  errorMsg?: string;
};

export type Insight = {
  name: InsightNameEnum;
  label: string;
  description: string;
  tooltipDescription: string;
};

export type ContentionEvent = {
  executionID: string;
  fingerprintID: string;
  waitingStmtID: string;
  waitingStmtFingerprintID: string;
  queries: string[];
  startTime: Moment;
  contentionTimeMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  execType: InsightExecEnum;
  stmtInsightEvent: StmtInsightEvent;
};

export const highContentionInsight = (
  execType: InsightExecEnum,
  latencyThresholdMs?: number,
  contentionDuration?: number,
): Insight => {
  let waitDuration: string;
  if (
    latencyThresholdMs &&
    contentionDuration &&
    contentionDuration < latencyThresholdMs
  ) {
    waitDuration = `${contentionDuration}ms`;
  } else if (!latencyThresholdMs) {
    waitDuration =
      "longer than the value of the 'sql.insights.latency_threshold' cluster setting";
  } else {
    waitDuration = `longer than ${latencyThresholdMs}ms`;
  }
  const description = `This ${execType} waited on other ${execType}s to execute for ${waitDuration}.`;
  return {
    name: InsightNameEnum.highContention,
    label: InsightEnumToLabel.get(InsightNameEnum.highContention),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const slowExecutionInsight = (
  execType: InsightExecEnum,
  latencyThreshold?: number,
): Insight => {
  let threshold = latencyThreshold + "ms";
  if (!latencyThreshold) {
    threshold =
      "the value of the 'sql.insights.latency_threshold' cluster setting";
  }
  const description = `This ${execType} took longer than ${threshold} to execute.`;
  return {
    name: InsightNameEnum.slowExecution,
    label: InsightEnumToLabel.get(InsightNameEnum.slowExecution),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const planRegressionInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} was slow because we picked the wrong plan, ` +
    `possibly due to outdated statistics, the statement using different literals or ` +
    `search conditions, or a change in the database schema.`;
  return {
    name: InsightNameEnum.planRegression,
    label: InsightEnumToLabel.get(InsightNameEnum.planRegression),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const suboptimalPlanInsight = (execType: InsightExecEnum): Insight => {
  let description = "";
  switch (execType) {
    case InsightExecEnum.STATEMENT:
      description =
        `This statement was slow because a good plan was unavailable; ` +
        `possible reasons include outdated statistics or missing indexes.`;
      break;
    case InsightExecEnum.TRANSACTION:
      description =
        "This transaction was slow because a good plan was unavailable for some " +
        "statement(s) in this transaction; possible reasons include outdated " +
        "statistics or missing indexes.";
      break;
  }
  return {
    name: InsightNameEnum.suboptimalPlan,
    label: InsightEnumToLabel.get(InsightNameEnum.suboptimalPlan),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const highRetryCountInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} has being retried more times than the value of the ` +
    `'sql.insights.high_retry_count.threshold' cluster setting.`;
  return {
    name: InsightNameEnum.highRetryCount,
    label: InsightEnumToLabel.get(InsightNameEnum.highRetryCount),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const failedExecutionInsight = (execType: InsightExecEnum): Insight => {
  const description =
    `This ${execType} execution failed completely, due to contention, resource ` +
    `saturation, or syntax errors.`;
  return {
    name: InsightNameEnum.failedExecution,
    label: InsightEnumToLabel.get(InsightNameEnum.failedExecution),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

/**
 * getInsightForSlowExecution returns an insight object with labels and descriptions
 * based on the problem, causes for the problem, and the execution type.
 * @param cause an enum value dictating a cause for the insight
 * @param execType execution type
 * @param latencyThreshold optional parameter used to describe the threshold for slow latencies
 * @param contentionDuration optional parameter used to describe the amount of contention experienced
 * @returns an insight object
 */
export const getInsightForSlowExecution = (
  cause: cockroach.sql.insights.Cause,
  execType: InsightExecEnum,
  latencyThreshold?: number,
  contentionDuration?: number,
): Insight => {
  switch (cause) {
    case InsightCause.HighContention:
      return highContentionInsight(
        execType,
        latencyThreshold,
        contentionDuration,
      );
    case InsightCause.PlanRegression:
      return planRegressionInsight(execType);
    case InsightCause.SuboptimalPlan:
      return suboptimalPlanInsight(execType);
    case InsightCause.HighRetryCount:
      return highRetryCountInsight(execType);
    case InsightCause.Unset:
      return slowExecutionInsight(execType, latencyThreshold);
    default:
      return slowExecutionInsight(execType, latencyThreshold);
  }
};

export const InsightExecOptions = new Map<string, string>([
  [InsightExecEnum.STATEMENT.toString(), "Statement Executions"],
  [InsightExecEnum.TRANSACTION.toString(), "Transaction Executions"],
]);

export const InsightEnumToLabel = new Map<string, string>([
  [InsightNameEnum.highContention.toString(), "High Contention"],
  [InsightNameEnum.slowExecution.toString(), "Slow Execution"],
  [InsightNameEnum.suboptimalPlan.toString(), "Suboptimal Plan"],
  [InsightNameEnum.highRetryCount.toString(), "High Retry Count"],
  [InsightNameEnum.failedExecution.toString(), "Failed Execution"],
]);

export type WorkloadInsightEventFilters = Pick<
  Filters,
  "app" | "workloadInsightType"
>;

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
  execution?: ExecutionDetails;
  details?: insightDetails;
}

export interface indexDetails {
  table: string;
  schema: string;
  indexID: number;
  indexName: string;
  lastUsed?: string;
}

// These are the fields used for workload insight recommendations.
export interface ExecutionDetails {
  application?: string;
  databaseName?: string;
  elapsedTimeMillis?: number;
  contentionTimeMs?: number;
  fingerprintID?: string;
  implicit?: boolean;
  indexRecommendations?: string[];
  retries?: number;
  statement?: string;
  summary?: string;
  statementExecutionID?: string;
  transactionExecutionID?: string;
  execType?: InsightExecEnum;
  errorCode?: string;
  errorMsg?: string;
  status?: string;
}

export interface insightDetails {
  duration?: number;
  description: string;
}
