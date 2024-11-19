// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment, { Moment } from "moment-timezone";

import { Filters } from "../queryFilter";

const ContentionTypeEnum = cockroach.sql.contentionpb.ContentionType;

export type ContentionTypeKey = {
  [K in keyof typeof ContentionTypeEnum]: K;
}[keyof typeof ContentionTypeEnum];

// This enum corresponds to the string enum for `problems` in `cluster_execution_insights`
export enum InsightNameEnum {
  FAILED_EXECUTION = "FailedExecution",
  HIGH_CONTENTION = "HighContention",
  HIGH_RETRY_COUNT = "HighRetryCount",
  PLAN_REGRESSION = "PlanRegression",
  SUBOPTIMAL_PLAN = "SuboptimalPlan",
  SLOW_EXECUTION = "SlowExecution",
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
  contentionType: ContentionTypeKey;
};

// The return type of getTxnInsightsContentionDetailsApi.
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
    name: InsightNameEnum.HIGH_CONTENTION,
    label: InsightEnumToLabel.get(InsightNameEnum.HIGH_CONTENTION),
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
    name: InsightNameEnum.SLOW_EXECUTION,
    label: InsightEnumToLabel.get(InsightNameEnum.SLOW_EXECUTION),
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
    name: InsightNameEnum.PLAN_REGRESSION,
    label: InsightEnumToLabel.get(InsightNameEnum.PLAN_REGRESSION),
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
    name: InsightNameEnum.SUBOPTIMAL_PLAN,
    label: InsightEnumToLabel.get(InsightNameEnum.SUBOPTIMAL_PLAN),
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
    name: InsightNameEnum.HIGH_RETRY_COUNT,
    label: InsightEnumToLabel.get(InsightNameEnum.HIGH_RETRY_COUNT),
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
    name: InsightNameEnum.FAILED_EXECUTION,
    label: InsightEnumToLabel.get(InsightNameEnum.FAILED_EXECUTION),
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const getInsightFromCause = (
  cause: string,
  execOption: InsightExecEnum,
  latencyThreshold?: number,
  contentionDuration?: number,
): Insight => {
  switch (cause) {
    case InsightNameEnum.HIGH_CONTENTION:
      return highContentionInsight(
        execOption,
        latencyThreshold,
        contentionDuration,
      );
    case InsightNameEnum.FAILED_EXECUTION:
      return failedExecutionInsight(execOption);
    case InsightNameEnum.PLAN_REGRESSION:
      return planRegressionInsight(execOption);
    case InsightNameEnum.SUBOPTIMAL_PLAN:
      return suboptimalPlanInsight(execOption);
    case InsightNameEnum.HIGH_RETRY_COUNT:
      return highRetryCountInsight(execOption);
    default:
      return slowExecutionInsight(execOption, latencyThreshold);
  }
};

export const InsightExecOptions = new Map<string, string>([
  [InsightExecEnum.STATEMENT.toString(), "Statement Executions"],
  [InsightExecEnum.TRANSACTION.toString(), "Transaction Executions"],
]);

export const InsightEnumToLabel = new Map<string, string>([
  [InsightNameEnum.HIGH_CONTENTION.toString(), "High Contention"],
  [InsightNameEnum.SLOW_EXECUTION.toString(), "Slow Execution"],
  [InsightNameEnum.SUBOPTIMAL_PLAN.toString(), "Suboptimal Plan"],
  [InsightNameEnum.HIGH_RETRY_COUNT.toString(), "High Retry Count"],
  [InsightNameEnum.FAILED_EXECUTION.toString(), "Failed Execution"],
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
  indexDetails?: IndexDetails;
  execution?: ExecutionDetails;
  details?: InsightDetails;
}

export interface IndexDetails {
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

export interface InsightDetails {
  duration?: number;
  description: string;
}

export enum StmtFailureCodesStr {
  RETRY_SERIALIZABLE = "40001",
}
