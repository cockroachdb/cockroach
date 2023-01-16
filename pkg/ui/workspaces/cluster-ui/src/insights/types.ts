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

// What we store in redux for txn contention insight events in
// the overview page.  It is missing information such as the
// blocking txn information.
export type TxnContentionInsightEvent = {
  transactionID: string;
  transactionFingerprintID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  contentionDuration: moment.Duration;
  contentionThreshold: number;
  application: string;
  execType: InsightExecEnum;
};

export type TxnContentionInsightEventResult = {
  error: string;
  transactions: TxnContentionInsightEvent[];
};

// Information about the blocking transaction and schema.
export type BlockedContentionDetails = {
  collectionTimeStamp: Moment;
  blockingExecutionID: string;
  blockingTxnFingerprintID: string;
  blockingQueries: string[];
  contendedKey: string;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  contentionTimeMs: number;
};

// TODO (xinhaoz) these fields should be placed into TxnInsightEvent
// once they are available for contention insights.  MergedTxnInsightEvent,
// (which marks these fields as optional) can then be deleted.
type UnavailableForTxnContention = {
  databaseName: string;
  username: string;
  priority: string;
  retries: number;
  implicitTxn: boolean;
  sessionID: string;
};

export type TxnInsightEvent = UnavailableForTxnContention & {
  transactionExecutionID: string;
  transactionFingerprintID: string;
  application: string;
  lastRetryReason?: string;
  contention?: moment.Duration;

  // The full list of statements in this txn, with their stmt
  // level insights (not all stmts in the txn may have one).
  // Ordered by startTime.
  statementInsights: StatementInsightEvent[];

  insights: Insight[]; // De-duplicated list of insights from statement level.
  queries: string[]; // We bubble this up from statementinsights for easy access, since txn contention details dont have stmt insights.
  startTime?: Moment; // TODO (xinhaoz) not currently available
  elapsedTimeMillis?: number; // TODO (xinhaoz) not currently available
  endTime?: Moment; // TODO (xinhaoz) not currently available
};

export type MergedTxnInsightEvent = Omit<
  TxnInsightEvent,
  keyof UnavailableForTxnContention
> &
  Partial<UnavailableForTxnContention>;

export type TxnContentionInsightDetails = {
  transactionExecutionID: string;
  queries: string[];
  insights: Insight[];
  startTime: Moment;
  totalContentionTimeMs: number;
  contentionThreshold: number;
  application: string;
  transactionFingerprintID: string;
  blockingContentionDetails: BlockedContentionDetails[];
  execType: InsightExecEnum;
  insightName: string;
};

export type TxnInsightDetails = Omit<MergedTxnInsightEvent, "contention"> & {
  totalContentionTimeMs?: number;
  contentionThreshold?: number;
  blockingContentionDetails?: BlockedContentionDetails[];
  execType: InsightExecEnum;
};

export type BlockedStatementContentionDetails = {
  blockingTxnID: string;
  durationInMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
};

// Does not contain transaction information.
// This is what is stored at the transaction insight level, shown
// on the txn insights overview page.
export type StatementInsightEvent = {
  statementExecutionID: string;
  statementFingerprintID: string;
  startTime: Moment;
  isFullScan: boolean;
  elapsedTimeMillis: number;
  totalContentionTime?: moment.Duration;
  contentionEvents?: BlockedStatementContentionDetails[];
  endTime: Moment;
  rowsRead: number;
  rowsWritten: number;
  causes: string[];
  problem: string;
  query: string;
  insights: Insight[];
  indexRecommendations: string[];
  planGist: string;
};

// StatementInsightEvent with their transaction level information.
// What we show in the stmt insights overview and details pages.
export type FlattenedStmtInsightEvent = StatementInsightEvent & {
  transactionExecutionID: string;
  transactionFingerprintID: string;
  implicitTxn: boolean;
  sessionID: string;
  databaseName: string;
  username: string;
  lastRetryReason?: string;
  priority: string;
  retries: number;
  application: string;
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
  queries: string[];
  startTime: Moment;
  contentionTimeMs: number;
  schemaName: string;
  databaseName: string;
  tableName: string;
  indexName: string;
  execType: InsightExecEnum;
};

export const highContentionInsight = (
  execType: InsightExecEnum,
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
    label: "Slow Execution",
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
    label: "Plan Regression",
    description: description,
    tooltipDescription:
      description + ` Click the ${execType} execution ID to see more details.`,
  };
};

export const suboptimalPlanInsight = (execType: InsightExecEnum): Insight => {
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

export const highRetryCountInsight = (execType: InsightExecEnum): Insight => {
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

export const failedExecutionInsight = (execType: InsightExecEnum): Insight => {
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

export const getInsightFromCause = (
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
  databaseName?: string;
  elapsedTimeMillis?: number;
  contentionTime?: number;
  fingerprintID?: string;
  implicit?: boolean;
  indexRecommendations?: string[];
  retries?: number;
  statement?: string;
  summary?: string;
}

export interface insightDetails {
  duration?: number;
  description: string;
}
